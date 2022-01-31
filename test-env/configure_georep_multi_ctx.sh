#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
norestart="${2:-1}"

: "${CONTEXT_A:="context-a"}"
: "${CONTEXT_B:="context-b"}"

georep_tenant="georep"
georep_namespace="${georep_tenant}/replicated"
georep_topicbase="perftest"
georep_topic="persistent://${georep_namespace}/${georep_topicbase}"
partition_count=10

function set_current_cluster() {
    kubectl config use-context "$1"
}
set_current_cluster "$CONTEXT_A"

function get_cluster_id (){
    local is_same=${1:-1}
    if [[ $is_same -eq 1 ]]; then
        echo "pulsar"
    else
        echo "cluster-$(ctx=$(kubectl config current-context); echo ${ctx/*-})"
    fi
}

function run_command_in_cluster() {
    local admin_script="$1"
    local flags="${2:-"-x"}"
    kubectl exec -i -n "pulsar" "$(kubectl get pod -n "pulsar" -l component=broker -o name | head -1)" -c "$(get_cluster_id 1)-broker" -- bash -c $flags "export PATH=/pulsar/bin:\$PATH; ${admin_script}"
}

function stop_georep() {
    local own_cluster_name="$1"

    read -r -d '' admin_script <<EOF
pulsar-admin namespaces set-clusters -c ${own_cluster_name} ${georep_namespace}
EOF

    run_command_in_cluster "${admin_script}"
}

function delete_georep_peer_cluster() {
    local peer_cluster_name="$1"

    read -r -d '' admin_script <<EOF
# delete peer cluster
pulsar-admin clusters delete ${peer_cluster_name}
EOF

    run_command_in_cluster "${admin_script}"
}

function unload_georep_topic() {
    read -r -d '' admin_script <<EOF
source /pulsar/conf/client.conf
pulsar_jwt=\$(cat /pulsar/token-superuser-stripped.jwt)

# unload individual partitions
curl -L --retry 3 -k -H "Authorization: Bearer \${pulsar_jwt}" --parallel --parallel-immediate --parallel-max 10 -X PUT "\${webServiceUrl}admin/v2/persistent/${georep_namespace}/${georep_topicbase}-partition-[0-$((partition_count - 1))]/unload"
# unload partitioned topic
curl -L --retry 3 -k -H "Authorization: Bearer \${pulsar_jwt}" -X PUT "\${webServiceUrl}admin/v2/persistent/${georep_namespace}/${georep_topicbase}/unload"
EOF

    run_command_in_cluster "${admin_script}"
}

function delete_georep_topic() {
    read -r -d '' admin_script <<EOF
source /pulsar/conf/client.conf
pulsar_jwt=\$(cat /pulsar/token-superuser-stripped.jwt)

# delete partitioned topic
curl -L --retry 3 -k -H "Authorization: Bearer \${pulsar_jwt}" -X DELETE "\${webServiceUrl}admin/v2/persistent/${georep_namespace}/${georep_topicbase}/partitions?force=true&deleteSchema=false"
# delete individual partitions
curl -L --retry 3 -k -H "Authorization: Bearer \${pulsar_jwt}" --parallel --parallel-immediate --parallel-max 10 -X DELETE "\${webServiceUrl}admin/v2/persistent/${georep_namespace}/${georep_topicbase}-partition-[0-$((partition_count - 1))]?force=true&deleteSchema=false"
EOF

    run_command_in_cluster "${admin_script}"
}

function delete_georep_namespace() {
    read -r -d '' admin_script <<EOF
source /pulsar/conf/client.conf
pulsar_jwt=\$(cat /pulsar/token-superuser-stripped.jwt)

# force delete namespace
curl -L --retry 3 -k -H "Authorization: Bearer \${pulsar_jwt}" -X DELETE "\${webServiceUrl}admin/v2/namespaces/${georep_namespace}?force=true"

pulsar-admin namespaces delete ${georep_namespace}
pulsar-admin tenants delete ${georep_tenant}
EOF

    run_command_in_cluster "${admin_script}"
}


function set_up_georep (){
    local own_cluster_name="$1"
    local peer_cluster_dns="$2"
    local peer_cluster_name="$3"
    local peer_cluster_token="$4"
   
    echo "Setting up georep for peer cluster ${peer_cluster_name}, creating ${georep_topic} and removing the existing subscription..."

    read -r -d '' admin_script <<EOF
pulsar-admin tenants create ${georep_tenant}
pulsar-admin namespaces create -b 64 ${georep_namespace}

# pulsar-admin clusters create --tls-enable --tls-allow-insecure --tls-trust-certs-filepath /pulsar/certs/tls.crt --auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken --auth-parameters "token:${peer_cluster_token}" --broker-url-secure pulsar+ssl://${peer_cluster_dns}:6651 --url-secure https://${peer_cluster_dns}:8443 ${peer_cluster_name}

pulsar-admin clusters create --broker-url pulsar://${peer_cluster_dns}:6650 --url http://${peer_cluster_dns}:8080 --auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken --auth-parameters "token:${peer_cluster_token}" ${peer_cluster_name}

# pulsar-admin clusters create --broker-url pulsar://${peer_cluster_dns}:6650 --url http://${peer_cluster_dns}:8080 ${peer_cluster_name}

pulsar-admin tenants update --allowed-clusters "${cluster_a_id},${cluster_b_id}" ${georep_tenant}
pulsar-admin namespaces set-clusters -c ${own_cluster_name},${peer_cluster_name} ${georep_namespace}

pulsar-admin namespaces get-clusters ${georep_namespace}
pulsar-admin topics create-partitioned-topic -p $partition_count ${georep_topic}
EOF

    run_command_in_cluster "${admin_script}"
}

function unconfigure_georep() {
    echo "Unconfiguring geo-replication..."
    if [[ $norestart -ne 1 ]]; then
        echo "Restarting between stages."
    else
        echo "No restarts will be used."
    fi

    # georeplication has to be stopped before deleting topics
    set_current_cluster $CONTEXT_A
    stop_georep "${cluster_a_id}"
    set_current_cluster $CONTEXT_B
    stop_georep "${cluster_b_id}"
    set_current_cluster $CONTEXT_A
    delete_georep_peer_cluster "${cluster_b_id}"
    set_current_cluster $CONTEXT_B
    delete_georep_peer_cluster "${cluster_a_id}"

    # wait for georep to stop
    echo "Wait 10 seconds..."
    sleep 10

    # delete topics
    set_current_cluster $CONTEXT_A
    unload_georep_topic
    delete_georep_topic
    delete_georep_namespace

    if [[ $norestart -ne 1 ]]; then
        # restart brokers
        kubectl rollout restart deployment -n pulsar cluster-a-broker
    fi

    set_current_cluster $CONTEXT_B
    unload_georep_topic
    delete_georep_topic
    delete_georep_namespace

    if [[ $norestart -ne 1 ]]; then
        # restart brokers
        kubectl rollout restart deployment -n pulsar cluster-b-broker

        echo "Wait for brokers to restart..."
        set_current_cluster $CONTEXT_A
        kubectl rollout status deployment -n pulsar cluster-a-broker
        set_current_cluster $CONTEXT_B
        kubectl rollout status deployment -n pulsar cluster-b-broker
    fi

    echo "Wait 10 seconds"
    sleep 10
}

function expose_pulsar_proxy (){
    local node_running_proxy="$(kubectl -n pulsar get po -l component=proxy \
                                                                     -o jsonpath="{.items[0].spec.nodeName}" )"
    local node_running_proxy_ip="$(kubectl get node \
                                                             "${node_running_proxy}" -o jsonpath="{.status.addresses[0].address}")"
    
    kubectl patch "$(kubectl -n pulsar get svc -l component=proxy -o name)" -n pulsar -p \
            "{\"spec\": {\"type\": \"LoadBalancer\", \"externalIPs\": [\"${node_running_proxy_ip}\"]}}"
}

function initialise_georeplication_variables (){
    export cluster_a_id="${CLUSTER_A_NAME}"
    
    kubectl config use-context "$(nutils_get_cluster_context "${CLUSTER_A_NAME}")" && \
        export cluster_a_hostname="$(kubectl -n pulsar get po -l component=proxy \
                                                                             -o jsonpath="{.items[0].spec.nodeName}" )" && \
        echo "cluster_a_hostname=${cluster_a_hostname}" || \
        echo "Error: impossible to set cluster_a_hostname"
    
    export cluster_b_id="${CLUSTER_B_NAME}"
    
    kubectl config use-context "$(nutils_get_cluster_context "${CLUSTER_B_NAME}")" && \
        export cluster_b_hostname="$(kubectl -n pulsar get po -l component=proxy \
                                                                             -o jsonpath="{.items[0].spec.nodeName}" )"  && \
        echo "cluster_b_hostname=${cluster_b_hostname}" || \
        echo "Error: impossible to set cluster_b_hostname"
}

function configure_georep() {
    echo "Configuring geo-replication..."

    set_current_cluster $CONTEXT_A
    token_a=$(run_command_in_cluster "cat /pulsar/token-superuser/superuser.jwt" "")
    set_current_cluster $CONTEXT_B
    token_b=$(run_command_in_cluster "cat /pulsar/token-superuser/superuser.jwt" "")

    set_current_cluster $CONTEXT_A
    expose_pulsar_proxy
    
    set_current_cluster $CONTEXT_B
    expose_pulsar_proxy
    
    initialise_georeplication_variables
    
    # setup georeplication and create topics
    set_current_cluster $CONTEXT_A
    set_up_georep "${cluster_a_id}" "${cluster_b_hostname}" "${cluster_b_id}" "${token_b}"
    set_current_cluster $CONTEXT_B
    set_up_georep "${cluster_b_id}" "${cluster_a_hostname}" "${cluster_a_id}" "${token_a}"

    echo "Wait 15 seconds..."
    sleep 15
}

# if [[ $command == "start" ]]; then
#     configure_georep
# else
#     unconfigure_georep
# fi
