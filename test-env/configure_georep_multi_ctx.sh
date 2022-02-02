#!/usr/bin/env bash
restart="${1:-0}"
norestart="${2:-1}"

: "${CONTEXT_A:=""}"
: "${CONTEXT_B:=""}"

: "${FULLNAME_OVERRIDE_CONTEXT_A:=""}"
: "${FULLNAME_OVERRIDE_CONTEXT_B:=""}"

: "${NAMESPACE_CONTEXT_A:=""}"
: "${NAMESPACE_CONTEXT_B:=""}"

: "${SA_CONTEXT_A:=""}"
: "${SA_CONTEXT_B:=""}"

CLUSTER_A_ID="cluster-a"
CLUSTER_B_ID="cluster-b"

: "${CLUSTER_SWITCH_MODE:="kubeconfig"}"

export DIFFERENT_CLUSTER_IDS="${DIFFERENT_CLUSTER_IDS:=0}"

georep_tenant="georep"
georep_namespace="${georep_tenant}/replicated"
georep_topicbase="perftest"
georep_topic="persistent://${georep_namespace}/${georep_topicbase}"
partition_count=10

function switch_cluster() {
    
    local current_cluster="$1"

    if [[ $current_cluster == "${CONTEXT_A}" ]]; then
        echo "Switching to cluster-a"
    elif [[ $current_cluster == "${CONTEXT_B}" ]]; then
        echo "Switching to cluster-b"
    else
        echo "Unknown k8s context / kubeconfig: ${current_cluster} "
    fi
    
    if [[ $CLUSTER_SWITCH_MODE == "kubeconfig" ]]; then
        export KUBECONFIG="${current_cluster}"
        echo "KUBECONFIG=$KUBECONFIG"
    elif [[ $CLUSTER_SWITCH_MODE == "context" ]]; then
        kubectl config use-context "${current_cluster}"
    fi
    
}

function is_cluster() {
    
    local current_cluster="$1"
    if [[ $CLUSTER_SWITCH_MODE == "kubeconfig" ]]; then
        [[ ${KUBECONFIG} == "${current_cluster}" ]] && echo 1 || echo 0
    elif [[ $CLUSTER_SWITCH_MODE == "context" ]]; then
        [[ $(kubectl config current-context) == "${current_cluster}" ]] && echo 1 || echo 0
    fi
    
}

function get_cluster_fn_override (){
    if [[ $(is_cluster ${CONTEXT_A}) -eq 1 ]]; then
        echo "$FULLNAME_OVERRIDE_CONTEXT_A"
    elif [[ $(is_cluster ${CONTEXT_B}) -eq 1 ]]; then
        echo "$FULLNAME_OVERRIDE_CONTEXT_B"
    else
        return 1
    fi
}

function get_cluster_namespace (){
    if [[ $(is_cluster ${CONTEXT_A}) -eq 1 ]]; then
        echo "$NAMESPACE_CONTEXT_A"
    elif [[ $(is_cluster ${CONTEXT_B}) -eq 1 ]]; then
        echo "$NAMESPACE_CONTEXT_B"
    else
        return 1
    fi
}

function get_cluster_sa (){
    if [[ $(is_cluster ${CONTEXT_A}) -eq 1 ]]; then
        echo "$SA_CONTEXT_A"
    elif [[ $(is_cluster ${CONTEXT_B}) -eq 1 ]]; then
        echo "$SA_CONTEXT_B"
    else
        return 1
    fi
}

function kubectl_user (){
    local verbose=${VERBOSE:-0}

    local kubectl_args=("-n" "$(get_cluster_namespace)")
    
    
    if [[ $NO_USER -eq 0 ]]; then
        kubectl_args+=("--as-group=system:authenticated"
                       "--as=$(get_cluster_namespace):$(get_cluster_sa)")
    fi

    kubectl_args+=("${@}")
    
    if [[ $verbose -eq 1 ]]; then
        echo "+ kubectl ${kubectl_args[*]}"
    fi
    
    kubectl "${kubectl_args[@]}"
}

function run_command_in_cluster() {
    local admin_script="$1"
    local flags="${2:-"-x"}"
    local args=("exec" "-i" "$(kubectl_user get pod -l component=broker -o name)" "-c" "$(get_cluster_fn_override)-broker" "--" "bash" "-c" "$flags" "export PATH=/pulsar/bin:\$PATH; ${admin_script}")
    kubectl_user "${args[@]}"
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

    local cluster_a_id=${CLUSTER_A_ID}
    echo "cluster_a_id=${cluster_a_id}"
    
    local cluster_b_id=${CLUSTER_B_ID}
    echo "cluster_b_id=${cluster_b_id}"
    
    # georeplication has to be stopped before deleting topics
    switch_cluster $CONTEXT_A
    stop_georep "${cluster_a_id}"
    switch_cluster $CONTEXT_B
    stop_georep "${cluster_b_id}"
    switch_cluster $CONTEXT_A
    delete_georep_peer_cluster "${cluster_b_id}"
    switch_cluster $CONTEXT_B
    delete_georep_peer_cluster "${cluster_a_id}"

    # wait for georep to stop
    echo "Wait 10 seconds..."
    sleep 10

    # delete topics
    switch_cluster $CONTEXT_A
    unload_georep_topic
    delete_georep_topic
    delete_georep_namespace

    if [[ $norestart -ne 1 ]]; then
        # restart brokers
        kubectl_user rollout restart deployment  cluster-a-broker
    fi

    switch_cluster $CONTEXT_B
    unload_georep_topic
    delete_georep_topic
    delete_georep_namespace

    if [[ $norestart -ne 1 ]]; then
        # restart brokers
        kubectl_user rollout restart deployment  cluster-b-broker

        echo "Wait for brokers to restart..."
        switch_cluster $CONTEXT_A
        kubectl_user rollout status deployment  cluster-a-broker
        switch_cluster $CONTEXT_B
        kubectl_user rollout status deployment  cluster-b-broker
    fi

    echo "Wait 10 seconds"
    sleep 10
}

function expose_pulsar_proxy (){
    local node_running_proxy="$(kubectl_user  get po -l component=proxy \
                                                                     -o jsonpath="{.items[0].spec.nodeName}" )"
    local node_running_proxy_ip="$(kubectl_user get node \
                                                             "${node_running_proxy}" -o jsonpath="{.status.addresses[0].address}")"
    
    kubectl_user patch "$(  kubectl_user  get svc -l component=proxy -o name)"  -p \
            "{\"spec\": {\"type\": \"LoadBalancer\", \"externalIPs\": [\"${node_running_proxy_ip}\"]}}"
}

function initialise_georeplication_variables (){
    export cluster_a_id="${CLUSTER_A_NAME}"
    
    kubectl_user config use-context "$(nutils_get_cluster_context "${CLUSTER_A_NAME}")" && \
        export cluster_a_hostname="$(  kubectl_user  get po -l component=proxy \
                                                                             -o jsonpath="{.items[0].spec.nodeName}" )" && \
        echo "cluster_a_hostname=${cluster_a_hostname}" || \
        echo "Error: impossible to set cluster_a_hostname"
    
    export cluster_b_id="${CLUSTER_B_NAME}"
    
    kubectl_user config use-context "$(nutils_get_cluster_context "${CLUSTER_B_NAME}")" && \
        export cluster_b_hostname="$(kubectl_user  get po -l component=proxy \
                                                                             -o jsonpath="{.items[0].spec.nodeName}" )"  && \
        echo "cluster_b_hostname=${cluster_b_hostname}" || \
        echo "Error: impossible to set cluster_b_hostname"
}

function configure_georep() {
    echo "Configuring geo-replication..."

    switch_cluster $CONTEXT_A
    local token_a=$(run_command_in_cluster "cat /pulsar/token-superuser/superuser.jwt" "")
    switch_cluster $CONTEXT_B
    local token_b=$(run_command_in_cluster "cat /pulsar/token-superuser/superuser.jwt" "")

    switch_cluster $CONTEXT_A
    expose_pulsar_proxy
    
    switch_cluster $CONTEXT_B
    expose_pulsar_proxy
    
    switch_cluster $CONTEXT_A
    local cluster_a_hostname="$(kubectl_user get po -l component=proxy \
                                              -o jsonpath="{.items[0].spec.nodeName}" )"
    echo "cluster_a_hostname=${cluster_a_hostname}"
    
    local cluster_a_id=${CLUSTER_A_ID}
    echo "cluster_a_id=${cluster_a_id}"

    switch_cluster $CONTEXT_B
    local cluster_b_hostname="$(kubectl_user get po -l component=proxy \
                                              -o jsonpath="{.items[0].spec.nodeName}" )"
    echo "cluster_b_hostname=${cluster_b_hostname}"

    local cluster_b_id=${CLUSTER_B_ID}
    echo "cluster_b_id=${cluster_b_id}"
    
    # setup georeplication and create topics
    switch_cluster $CONTEXT_A
    set_up_georep "${cluster_a_id}" "${cluster_b_hostname}" "${cluster_b_id}" "${token_b}"
    switch_cluster $CONTEXT_B
    set_up_georep "${cluster_b_id}" "${cluster_a_hostname}" "${cluster_a_id}" "${token_a}"

    echo "Wait 15 seconds..."
    sleep 15
}

if [[ $restart -eq 0 ]]; then
    configure_georep
else
    unconfigure_georep
fi
