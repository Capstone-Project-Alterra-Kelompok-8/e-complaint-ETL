import pymysql
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.context import get_run_context

## EXTRACT DATA TASKS

@task
def load_env():
    load_dotenv()
    config = {
            'DB_HOST': Secret.load("db-host").get(),
            'DB_USERNAME':  Secret.load("db-username").get(),
            'DB_PASSWORD':  Secret.load("db-password").get(),
            'DB_PORT':  int(Secret.load("db-port").get()),
            'DB_NAME_BE':  Secret.load("db-name").get(),
            'GOOGLE_APPLICATION_CREDENTIALS': Secret.load("gcp-credentials").get()
        }
    return config

@task
def connect_to_db(config):
    conn = pymysql.connect(
        host=config['DB_HOST'],
        user=config['DB_USERNAME'],
        password=config['DB_PASSWORD'],
        database=config['DB_NAME_BE'],
        port=int(config['DB_PORT'])
    )
    return conn

@task
def fetch_data(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    return data

## TRANSFORM DATA TASKS

@task
def process_complaint_id(df, column_name):
    def complaint_id_format(c_id):
        if not c_id.startswith("C-"):
            return "C-" + c_id
        else:
            return c_id
    df[column_name] = df[column_name].apply(complaint_id_format)
    return df

@task
def filter_and_convert(df, column_name):
    df[column_name] = df[column_name].astype(str)
    mask = df[column_name].str.startswith(('0', '8'))
    filtered_df = df[mask].copy()
    filtered_df[column_name] = filtered_df[column_name].astype(int)
    return filtered_df

@task
def fill_nan(df, column_name, value):
    df[column_name] = df[column_name].fillna(value)
    return df

@task
def map_category_id_to_name(df, column_name, mapping):
    df[column_name] = df[column_name].map(mapping)
    return df

@task
def set_working_directory():
    desired_directory = "/home/raihan12/Documents/e-complaint-ETL"
    os.chdir(desired_directory)

@task
def save_data(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


## LOAD DATA TASKS

@task
def load_complaint_facts(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)
    
    dataframe['updated_at'] = partition_datetime

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("user_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("complaint_id", "STRING", "NULLABLE"),
            bigquery.SchemaField("complaint_process_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("admin_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("total_complaints", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_verification", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_onprogress", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_resolved", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_rejected", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_deleted", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_public", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("sum_private", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("process_time", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("user_rejected_complaints", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("user_resolved_complaints", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("last_complaints", "STRING", "NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

@task
def load_news_facts(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)

    dataframe['updated_at'] = partition_datetime

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("admin_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("news_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("least_followed_category", "STRING", "NULLABLE"),
            bigquery.SchemaField("least_followed_category_likes", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("most_followed_category", "STRING", "NULLABLE"),
            bigquery.SchemaField("most_followed_category_likes", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("last_created_news", "TIMESTAMP", "NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

@task
def load_complaints(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)
    
    for col in ['created_at', 'updated_at', 'deleted_at']:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    dataframe['updated_at'] = partition_datetime.strftime('%Y-%m-%d %H:%M:%S')

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("description", "STRING", "NULLABLE"),
            bigquery.SchemaField("address", "STRING", "NULLABLE"),
            bigquery.SchemaField("type", "STRING", "NULLABLE"),
            bigquery.SchemaField("total_likes", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP", "NULLABLE"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

@task
def load_users(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)
    
    for col in ['created_at', 'updated_at', 'deleted_at']:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    dataframe['updated_at'] = partition_datetime.strftime('%Y-%m-%d %H:%M:%S')

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField("email", "STRING", "NULLABLE"),
            bigquery.SchemaField("telephone_number", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP", "NULLABLE"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

@task
def load_admins(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)
    
    for col in ['created_at', 'updated_at', 'deleted_at']:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    dataframe['updated_at'] = partition_datetime.strftime('%Y-%m-%d %H:%M:%S')

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("name", "STRING", "NULLABLE"),
            bigquery.SchemaField("email", "STRING", "NULLABLE"),
            bigquery.SchemaField("telephone_number", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("is_super_admin", "BOOLEAN", "NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP", "NULLABLE"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

@task
def load_complaint_processes(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)
    
    for col in ['created_at', 'updated_at', 'deleted_at']:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    dataframe['updated_at'] = partition_datetime.strftime('%Y-%m-%d %H:%M:%S')

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("complaint_id", "STRING", "NULLABLE"),
            bigquery.SchemaField("status", "STRING", "NULLABLE"),
            bigquery.SchemaField("message", "STRING", "NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP", "NULLABLE"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

@task
def load_news(file_name, data_dir, dataset_id, table_id, google_credentials):
    client = bigquery.Client.from_service_account_json(google_credentials)
    
    partition_date_ymd = file_name.split('_')[-1].split('.')[0]
    partition_date = datetime.strptime(partition_date_ymd, '%Y-%m-%d').strftime('%Y%m%d')
    partition_datetime = datetime.strptime(partition_date_ymd, '%Y-%m-%d')
    
    table_ref = f"{dataset_id}.{table_id}${partition_date}"
    
    file_path = os.path.join(data_dir, file_name)
    dataframe = pd.read_csv(file_path)
    
    for col in ['created_at', 'updated_at', 'deleted_at']:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    dataframe['updated_at'] = partition_datetime.strftime('%Y-%m-%d %H:%M:%S')

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("category", "STRING", "NULLABLE"),
            bigquery.SchemaField("title", "STRING", "NULLABLE"),
            bigquery.SchemaField("total_likes", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP", "NULLABLE"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
            require_partition_filter=True
        ),
        source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f'Loaded {load_job.output_rows} rows into {table_ref}')

## FLOW
@flow
def etl_flow():
    CATEGORY_MAPPING = {
        1: "Kesehatan",
        2: "Pendidikan",
        3: "Kependudukan",
        4: "Keamanan",
        5: "Infrastruktur",
        6: "Lingkungan",
        7: "Transportasi"
    }

    set_working_directory()

    context = get_run_context()
    expected_start_time = context.flow_run.expected_start_time.strftime('%Y-%m-%d')
    data_directory = f'cleaned_data/{expected_start_time}'
    
    # Log the current working directory and environment information
    print("Current working directory:", os.getcwd())
    
    try:
        os.makedirs(data_directory, exist_ok=True)
        print(f"Successfully created directory: {data_directory}")
    except Exception as e:
        print(f"Failed to create directory {data_directory}: {e}")
        raise

    
    config = load_env()

    conn = connect_to_db(config)

    queries = {
        'complaint_facts': """
            WITH latest_status AS (
                SELECT
                    complaint_id,
                    status,
                    created_at,
                    ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY created_at DESC) AS rn
                FROM
                    complaint_processes
            )
            SELECT
                c.user_id,
                c.id AS complaint_id,
                cp.id AS complaints_process_id,
                a.id AS admin_id,
                (SELECT COUNT(id) FROM complaints) as total_complaints,
                (SELECT COUNT(status) FROM latest_status WHERE rn = 1 AND status = 'verifikasi') AS sum_verification,
                (SELECT COUNT(status) FROM latest_status WHERE rn = 1 AND status = 'on progress') AS sum_onprogress,
                (SELECT COUNT(status) FROM latest_status WHERE rn = 1 AND status = 'selesai') AS sum_resolved,
                (SELECT COUNT(status) FROM latest_status WHERE rn = 1 AND status = 'ditolak') AS sum_rejected,
                (SELECT COUNT(deleted_at) FROM complaints WHERE deleted_at IS NOT NULL) AS sum_deleted,
                (SELECT COUNT(type) FROM complaints WHERE type = 'public') AS sum_public,
                (SELECT COUNT(type) FROM complaints WHERE type = 'private') AS sum_private,
                DATEDIFF(
                    (SELECT MAX(created_at) FROM complaint_processes WHERE status IN ('selesai', 'ditolak') AND complaint_id = c.id),
                    (SELECT MAX(created_at) FROM complaint_processes WHERE status = 'verifikasi' AND complaint_id = c.id)
                ) AS process_time,
                COALESCE(u_rejected.count, 0) AS user_rejected_complaints,
                COALESCE(u_resolved.count, 0) AS user_resolved_complaints,
                lc.last_complaint_id AS last_complaints
            FROM
                complaints c
            JOIN
                complaint_processes cp ON c.id = cp.complaint_id
            JOIN
                users u ON c.user_id = u.id
            JOIN
                admins a ON cp.admin_id = a.id
            LEFT JOIN (
                SELECT user_id, COUNT(*) AS count
                FROM complaints
                WHERE status = 'ditolak'
                GROUP BY user_id
            ) u_rejected ON c.user_id = u_rejected.user_id
            LEFT JOIN (
                SELECT user_id, COUNT(*) AS count
                FROM complaints
                WHERE status = 'selesai'
                GROUP BY user_id
            ) u_resolved ON c.user_id = u_resolved.user_id
            LEFT JOIN (
                SELECT user_id, MAX(id) AS last_complaint_id
                FROM complaints
                GROUP BY user_id
            ) lc ON c.user_id = lc.user_id
            GROUP BY
                c.user_id, c.id, cp.id, a.id, u_rejected.count, u_resolved.count, lc.last_complaint_id
            ORDER BY
                user_id ASC;
        """,
        'news_facts': """
            WITH least_followed_category AS (
                SELECT category_id, SUM(total_likes) AS total_likes
                FROM news
                GROUP BY category_id
                ORDER BY SUM(total_likes) ASC
                LIMIT 1
            ),
            most_followed_category AS (
                SELECT category_id, SUM(total_likes) AS total_likes
                FROM news
                GROUP BY category_id
                ORDER BY SUM(total_likes) DESC
                LIMIT 1
            ),
            last_created_news AS (
                SELECT MAX(created_at) AS last_created_news
                FROM news
            )
            SELECT
                a.id,
                n.id AS news_id,
                lfc.category_id AS least_followed_category,
                lfc.total_likes AS least_followed_category_likes,
                mfc.category_id AS most_followed_category,
                mfc.total_likes AS most_followed_category_likes,
                lcn.last_created_news
            FROM
                news n
            JOIN
                admins a ON a.id = n.admin_id
            JOIN
                least_followed_category lfc ON 1=1
            JOIN
                most_followed_category mfc ON 1=1
            JOIN
                last_created_news lcn ON 1=1
            ORDER BY
                news_id ASC;
        """,
        'complaints': f"""
            SELECT
                id,
                description,
                address,
                type,
                total_likes,
                created_at,
                updated_at,
                deleted_at
            FROM 
                complaints
            WHERE
                updated_at = '{expected_start_time}'
        """,
        'users': f"""
            SELECT
                id,
                name,
                email,
                telephone_number,
                created_at,
                updated_at,
                deleted_at
            FROM 
                users
            WHERE
                updated_at = '{expected_start_time}'
        """,
        'admins': f"""
            SELECT
                id,
                name,
                email,
                telephone_number,
                is_super_admin,
                created_at,
                updated_at,
                deleted_at
            FROM 
                admins
            WHERE
                updated_at = '{expected_start_time}'
        """,
        'complaint_processes': f"""
            SELECT
                id,
                complaint_id,
                status,
                message,
                created_at,
                updated_at,
                deleted_at
            FROM 
                complaint_processes
            WHERE
                updated_at = '{expected_start_time}'
        """,
        'news': f"""
            SELECT
                id,
                category_id,
                title,
                total_likes,
                created_at,
                updated_at,
                deleted_at
            FROM 
                news
            WHERE
                updated_at = '{expected_start_time}'
        """
    }

    # Fetch data
    complaint_facts_data = fetch_data(queries['complaint_facts'], conn)
    news_facts_data = fetch_data(queries['news_facts'], conn)
    complaints_data = fetch_data(queries['complaints'], conn)
    users_data = fetch_data(queries['users'], conn)
    admin_data = fetch_data(queries['admins'], conn)
    complaint_processes_data = fetch_data(queries['complaint_processes'], conn)
    news_data = fetch_data(queries['news'], conn)


    # Convert to DataFrames
    complaint_facts_df = pd.DataFrame(complaint_facts_data, columns=[
        'user_id', 'complaint_id', 'complaint_process_id', 'admin_id', 'total_complaints',
        'sum_verification', 'sum_onprogress', 'sum_resolved', 'sum_rejected', 'sum_deleted',
        'sum_public', 'sum_private', 'process_time', 'user_rejected_complaints',
        'user_resolved_complaints', 'last_complaints'
    ])
    news_facts_df = pd.DataFrame(news_facts_data, columns=[
        'admin_id', 'news_id', 'least_followed_category', 'least_followed_category_likes',
        'most_followed_category', 'most_followed_category_likes', 'last_created_news'
    ])
    complaints_df = pd.DataFrame(complaints_data, columns=[
        'id', 'description', 'address', 'type', 'total_likes', 'created_at', 'updated_at', 'deleted_at'
    ])
    users_df = pd.DataFrame(users_data, columns=[
        'id', 'name', 'email', 'telephone_number', 'created_at', 'updated_at', 'deleted_at'
    ])
    admin_df = pd.DataFrame(admin_data, columns=[
        'id', 'name', 'email', 'telephone_number', 'is_super_admin', 'created_at', 'updated_at', 'deleted_at'
    ])
    complaint_processes_df = pd.DataFrame(complaint_processes_data, columns=[
        'id', 'complaint_id', 'status', 'message', 'created_at', 'updated_at', 'deleted_at'
    ])
    news_df = pd.DataFrame(news_data, columns=[
        'id', 'category', 'title', 'total_likes', 'created_at', 'updated_at', 'deleted_at'
    ])


    # Transform data
    complaint_facts_df = process_complaint_id(complaint_facts_df, 'complaint_id')
    complaints_df = process_complaint_id(complaints_df, 'id')
    complaint_processes_df = process_complaint_id(complaint_processes_df, 'complaint_id')

    users_df = filter_and_convert(users_df, 'telephone_number')
    admin_df = filter_and_convert(admin_df, 'telephone_number')

    complaint_facts_df = fill_nan(complaint_facts_df, 'process_time', 0)

    news_df = map_category_id_to_name(news_df, 'category', CATEGORY_MAPPING)
    news_facts_df = map_category_id_to_name(news_facts_df, 'least_followed_category', CATEGORY_MAPPING)
    news_facts_df = map_category_id_to_name(news_facts_df, 'most_followed_category', CATEGORY_MAPPING)

    # Save data
    save_data(complaint_facts_df, f"cleaned_data/{expected_start_time}/complaint_facts_{expected_start_time}.csv")
    save_data(news_facts_df, f"cleaned_data/{expected_start_time}/news_facts_{expected_start_time}.csv")
    save_data(complaints_df, f"cleaned_data/{expected_start_time}/complaints_{expected_start_time}.csv")
    save_data(users_df, f"cleaned_data/{expected_start_time}/users_{expected_start_time}.csv")
    save_data(admin_df, f"cleaned_data/{expected_start_time}/admins_{expected_start_time}.csv")
    save_data(complaint_processes_df, f"cleaned_data/{expected_start_time}/complaint_processes_{expected_start_time}.csv")
    save_data(news_df, f"cleaned_data/{expected_start_time}/news_{expected_start_time}.csv")

    # Load data to BigQuery
    for file_name in os.listdir(data_directory):
        if file_name.startswith('complaint_facts'):
            load_complaint_facts(file_name, data_directory, 'facts', 'complaint_facts', config['GOOGLE_APPLICATION_CREDENTIALS'])
        elif file_name.startswith('news_facts'):
            load_news_facts(file_name, data_directory, 'facts', 'news_facts', config['GOOGLE_APPLICATION_CREDENTIALS'])
        elif file_name.startswith('complaints'):
            load_complaints(file_name, data_directory, 'dim_tables', 'dim_complaints', config['GOOGLE_APPLICATION_CREDENTIALS'])
        elif file_name.startswith('admins'):
            load_admins(file_name, data_directory, 'dim_tables', 'dim_admins', config['GOOGLE_APPLICATION_CREDENTIALS'])
        elif file_name.startswith('users'):
            load_users(file_name, data_directory, 'dim_tables', 'dim_users', config['GOOGLE_APPLICATION_CREDENTIALS'])
        elif file_name.startswith('complaint_processes'):
            load_complaint_processes(file_name, data_directory, 'dim_tables', 'dim_complaint_processes', config['GOOGLE_APPLICATION_CREDENTIALS'])
        elif file_name.startswith('news'):
            load_news(file_name, data_directory, 'dim_tables', 'dim_news', config['GOOGLE_APPLICATION_CREDENTIALS'])

# Run the flow
if __name__ == "__main__":
    etl_flow()
