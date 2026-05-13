def test_api_key(api_key):
    assert api_key == "MOCK_API_KEY"

def test_channel_name(channel_handle):
    assert channel_handle == "MrBeast"

def test_mock_postgres_conn_vars(mock_postgres_conn_vars):
    conn= mock_postgres_conn_vars
    assert conn.login == "mock_username"    
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_schema"

def test_dags_integrity(dag_bag):
    # 1
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"
    print("============")
    print(dag_bag.import_errors)

    # 2
    expected_dag_ids = ['produce_json', 'update_db', 'data_quality']
    loaded_dag_ids = list(dag_bag.dags.keys())
    print("===========")
    print(dag_bag.dags.keys())
    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG '{dag_id}' not found in DAGBag"

    # 3
    assert dag_bag.size() == 3
    print("===========")
    print(dag_bag.size())

    # 4
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2
    }
    print("===========")
    for dag_id, dag in dag_bag.dags.items():
        expected_count = expected_task_counts.get(dag_id)
        actual_count = len(dag.tasks)
        assert expected_count == actual_count, f"DAG '{dag_id}' expected {expected_count} tasks but found {actual_count}"
        print(dag_id, len(dag.tasks))