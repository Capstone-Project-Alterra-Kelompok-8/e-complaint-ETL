import pymysql
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime


# load env file
load_dotenv()

# ambil data tanggal hari ini 
currentDateTime = datetime.now().strftime("%m-%d-%Y")

# koneksi database 
DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME_BE = os.environ.get("DB_NAME_BE")

config = {
    'host': DB_HOST,
    'user': DB_USERNAME,
    'password': DB_PASSWORD,
    'database': DB_NAME_BE,
    'port': int(DB_PORT) 
}

conn = pymysql.connect(**config)
cursor = conn.cursor()

# mengambil data dari database OLAP dan menggabungkannya untuk membentuk tabel fakta (complaint_facts)
cursor.execute("""
SELECT
    c.user_id,
    c.id AS complaint_id,
    cp.id AS complaints_process_id,
    a.id AS admin_id,
    (SELECT COUNT(id) FROM complaints) as total_complaints,
    (SELECT COUNT(status) FROM complaint_processes WHERE status = 'verifikasi') AS sum_verification,
    (SELECT COUNT(status) FROM complaint_processes WHERE status = 'on progress') AS sum_onprogress,
    (SELECT COUNT(status) FROM complaint_processes WHERE status = 'selesai') AS sum_resolved,
    (SELECT COUNT(status) FROM complaint_processes WHERE status = 'ditolak') AS sum_rejected,
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
""")

complaint_facts = cursor.fetchall()

complaint_facts = pd.DataFrame(complaint_facts, columns=['user_id',
                                      'complaint_id',
                                      'complaint_process_id',
                                      'admin_id',
                                      'total_complaints',
                                      'sum_verification',
                                      'sum_onprogress',
                                      'sum_resolved',
                                      'sum_rejected',
                                      'sum_deleted',
                                      'sum_public',
                                      'sum_private',
                                      'process_time',
                                      'user_rejected_complaints',
                                      'user_resolved_complaints',
                                      'last_complaints'])

complaint_facts.to_csv(f"complaint_facts/complaint_facts_{currentDateTime}.csv", index=False)

# mengambil data complaints dari database OLAP 
cursor.execute("""
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
""")

complaints_data = cursor.fetchall()

complaints_data = pd.DataFrame(complaints_data, columns=['id',
                                                    'description',
                                                    'address',
                                                    'type',
                                                    'total_likes',
                                                    'created_at',
                                                    'updated_at',
                                                    'deleted_at'])

complaints_data.to_csv(f"complaints/complaints_{currentDateTime}.csv", index=False)

# mengambil data users dari database OLAP 
cursor.execute("""
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
""")

users_data = cursor.fetchall()

users_data = pd.DataFrame(users_data, columns=['id',
                                                'name',
                                                'email',
                                                'telephone_number',
                                                'created_at',
                                                'updated_at',
                                                'deleted_at'])

users_data.to_csv(f"users/users_{currentDateTime}.csv", index=False)

# mengambil data admins dari database OLAP 
cursor.execute("""
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
""")

admin_data = cursor.fetchall()

admin_data = pd.DataFrame(admin_data, columns=['id',
                                                'name',
                                                'email',
                                                'telephone_number',
                                                'is_super_admin',
                                                'created_at',
                                                'updated_at',
                                                'deleted_at'])

admin_data.to_csv(f"admins/admins_{currentDateTime}.csv", index=False)