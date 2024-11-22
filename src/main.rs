use chrono::{Local, DateTime, Duration};
use rand::Rng;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use regex::Regex;
use lazy_static::lazy_static;
use sha2::{Sha256, Digest};
use hex;

lazy_static! {
    // 預先編譯正則表達式
    static ref VALUE_RE: Regex = Regex::new(r"[-+]?\d+(\.\d+)?|'[^']*'").unwrap();
}

// 定義 SQL 執行狀態
const STATUS: [&str; 2] = ["SUCCESS", "FAILURE"];

// 定義保存 conn_hash 資訊的結構
struct ConnHashInfo {
    conn_hash: String,
    current_entries: usize,
    stmt_id: usize,
    exec_id: usize,
    exec_time: DateTime<Local>,
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let default_entries = 100000;
    let num_entries = if let Some(pos) = args.iter().position(|x| x == "-r") {
        args.get(pos + 1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(default_entries)
    } else {
        default_entries
    };

    // 決定要產生的連線數量（10 到 50 個）
    let num_connections = rand::thread_rng().gen_range(10..=50);

    let file = File::create("sql_logs.tsv")?;
    let writer = Arc::new(Mutex::new(BufWriter::with_capacity(1 * 1024 * 1024, file)));

    // 寫入 TSV 標題
    {
        let mut writer = writer.lock().unwrap();
        writeln!(
            writer,
            "conn_hash\tstmt_id\texec_id\texec_time\tsql_type\texe_status\tdb_ip\tclient_ip\tclient_host\tapp_name\tdb_user\tsql_hash\tfrom_tbs\tselect_cols\tsql_stmt\tstmt_bind_vars"
        )?;
    }
    // =====================NEW CODE=====================
    // 創建執行緒
    let mut handles = vec![];
    for _ in 0..num_connections {
        let writer = Arc::clone(&writer);
        let num_entries = num_entries / num_connections;
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();

            // 初始化 conn_info
            let mut conn_info = ConnHashInfo {
                conn_hash: format!("conn_{}", rng.gen::<u64>()),
                current_entries: 0,
                stmt_id: 1,
                exec_id: 1,
                exec_time: Local::now() - Duration::days(rng.gen_range(3..=7)),
            };

            // 生成初始的 sql_stmt 和 sql_hash
            let sql_type = "SELECT";
            let table_names = [
                "users",
                "orders",
                "products",
                "departments",
                "employees",
                "salaries",
                "projects",
                "tasks",
                "events",
                "logs",
            ];
            let from_tbs = table_names[rng.gen_range(0..table_names.len())];
            let select_cols = "id, name, email";

            while conn_info.current_entries < num_entries {
                let db_ip = format!(
                    "192.168.{}.{}",
                    rng.gen_range(0..255),
                    rng.gen_range(0..255)
                );
                let client_ip = format!("10.0.{}.{}", rng.gen_range(0..255), rng.gen_range(0..255));
                let client_host = ["ERP_USER_HOST101", "ERP_USER_HOST102", "ERP_USER_HOST103", "ERP_USER_HOST104", "ERP_USER_HOST105"][rng.gen_range(0..5)].to_string();
                let app_name = match rng.gen_range(0..100) {
                    0..=50 => "ERP",
                    51..=80 => "WEB App",
                    81..=85 => "SQL Developer",
                    86..=90 => "Toad",
                    91..=95 => "PL/SQL Developer",
                    _ => "SQL*Plus",
                };
                let db_user = ["SYS", "SYSTEM", "HR", "SCOTT", "OE", "SH", "PM", "IX", "APEX_040000", "ANONYMOUS"][rng.gen_range(0..10)].to_string();


                // 每產生一筆 SQL，消耗一個 exec_id
                conn_info.exec_id += 1;

                // 檢查 exec_id 是否達到 300
                if conn_info.exec_id > 3 {
                    conn_info.stmt_id += 1;
                    conn_info.exec_id = 1;
                }

                // 檢查 stmt_id 是否達到 600
                if conn_info.stmt_id > rng.gen_range(30..=3000) || conn_info.exec_time >= Local::now() {
                    // 廢棄 conn_hash，生成新的 conn_hash，重置 stmt_id 和 exec_id
                    let mut hasher = Sha256::new();
                    conn_info.conn_hash = gen_conn_hash(&db_ip, &client_ip, &app_name);
                    conn_info.stmt_id = 1;
                    conn_info.exec_id = 1;
                    // 重置 exec_time
                    conn_info.exec_time = Local::now() - Duration::days(rng.gen_range(1..=7));
                }

                // 每次消耗都讓 exec_time 增加 1~30 秒
                let increment_seconds = rng.gen_range(1..=360);
                conn_info.exec_time =
                    conn_info.exec_time + Duration::seconds(increment_seconds as i64);



                // --todo start -----------------------------------------------------------------------

                // 生成其他欄位
                let sql_type = match rng.gen_range(0..100) {
                    0..=15 => "INSERT",
                    16..=25 => "UPDATE",
                    26..=30 => "DELETE",
                    31..=33 => "ALTER",
                    _ => "SELECT",
                };

                let exe_status = STATUS[rng.gen_range(0..STATUS.len())];
  
                let table_names = ["users", "orders", "products", "departments", "employees", "salaries", "projects", "tasks", "events", "logs"];
                let from_tbs = table_names[rng.gen_range(0..table_names.len())];
                let select_cols = generate_select_cols(sql_type);
                let sql_stmt = generate_sql_stmt(sql_type, from_tbs, &select_cols);

                let sql_hash = gen_sql_hash(&sql_stmt);

                let bind_vars_example = extract_where_values(&sql_stmt);
                // --todo end-----------------------------------------------------------------------



                // 寫入資料
                {
                    let mut writer = writer.lock().unwrap();
                    writeln!(
                        writer,
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        conn_info.conn_hash,
                        conn_info.stmt_id,
                        conn_info.exec_id,
                        conn_info.exec_time.format("%Y-%m-%d %H:%M:%S"),
                        sql_type,
                        exe_status,
                        db_ip,
                        client_ip,
                        client_host,
                        app_name,
                        db_user,
                        sql_hash,
                        from_tbs,
                        select_cols,
                        sql_stmt,
                        bind_vars_example
                    )
                    .unwrap();
                }

                conn_info.current_entries += 1;
            }
        });
        handles.push(handle);
    }

    // 等待所有執行緒完成
    for handle in handles {
        handle.join().unwrap();
    }

    // 確保資料寫入檔案
    {
        let mut writer = writer.lock().unwrap();
        writer.flush()?;
    }
    println!("SQL logs generated and saved to sql_logs.tsv");
    Ok(())
}

fn gen_conn_hash(db_ip: &str, client_ip: &str, app_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(db_ip.as_bytes());
    hasher.update(client_ip.as_bytes());
    hasher.update(app_name.as_bytes());
    let result = hex::encode(hasher.finalize());
    let result = &result[..32]; // 只取前8個字節
    format!("conn_{}", result)
}

fn gen_sql_hash(sql_stmt: &str) -> String {
    let cleaned_sql_stmt = VALUE_RE.replace_all(sql_stmt, "");
    let mut hasher = Sha256::new();
    hasher.update(cleaned_sql_stmt.as_bytes());
    let result = hex::encode(hasher.finalize());
    let result = &result[..16]; // 只取前8個字節
    format!("sql_{}", result)
}

// 以下是您的其他函式（請確保它們被定義）
fn generate_select_cols(sql_type: &str) -> String {
    if sql_type == "SELECT" {
        let columns = ["id", "produc_name", "age", "salary","commission", "product","price", "event_date", "department", "comm", "creation_dae", "created_by", "updated_date", "updated_by"];
        let mut rng = rand::thread_rng();
        let mut cols: Vec<String> = Vec::new();
        for _ in 0..rng.gen_range(1..columns.len()) {
            cols.push(columns[rng.gen_range(0..columns.len())].to_string());
        }
        cols.join(", ")
    } else {
        String::from("N/A")
    }
}

fn generate_sql_stmt(sql_type: &str, table: &str, select_cols: &str) -> String {
    let mut rng = rand::thread_rng();
    match sql_type {
        "SELECT" => {
            let where_clause = generate_where_clause();
            format!(
                "SELECT {} FROM {} {}",
                select_cols,
                table,
                where_clause
            )
        }
        "INSERT" => {
            let values = format!(
                "({}, '{}', {}, {})",
                rng.gen_range(1..100),
                ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis"][rng.gen_range(0..5)],
                rng.gen_range(20..50),
                rng.gen_range(3000..10000)
            );
            format!(
                "INSERT INTO {} (id, name, age, salary) VALUES {}",
                table, values
            )
        }
        "UPDATE" => {
            let set_clause = format!("SET salary = {}", rng.gen_range(3000..10000));
            let where_clause = generate_where_clause();
            format!("UPDATE {} {} {}", table, set_clause, where_clause)
        }
        "DELETE" => {
            let where_clause = generate_where_clause();
            format!("DELETE FROM {} {}", table, where_clause)
        }
        "ALTER" => {
            let alter_clause = format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                table,
                ["attribute01", "attribute02", "atttribute03"][rng.gen_range(0..3)],
                ["VARCHAR(255)", "INT", "DATE"][rng.gen_range(0..3)]
            );
            format!("{}", alter_clause)
        }
        _ => String::new(),
    }
}

fn extract_where_values(sql: &str) -> String {
    // Find "WHERE" clause manually to avoid regex for this part
    if let Some(where_start) = sql.to_uppercase().find("WHERE") {
        // Extract the WHERE clause substring
        let where_clause = &sql[where_start + 5..]; // Skip "WHERE"

        // Use precompiled regex to find matches
        let mut result = String::new();
        for cap in VALUE_RE.captures_iter(where_clause) {
            if !result.is_empty() {
                result.push_str(", ");
            }
            result.push_str(&cap[0]);
        }

        return result;
    }

    // If no WHERE clause is found, return an empty string
    String::new()
}

// 隨機生成 WHERE 子句
fn generate_where_clause() -> String {
    let mut rng = rand::thread_rng();
    let conditions = [
        format!("age > {}", rng.gen_range(20..50)),
        format!("salary < {}", rng.gen_range(3000..10000)),
        format!(
            "department = '{}'",
            ["HR", "Engineering", "Sales"][rng.gen_range(0..3)]
        ),
        format!("commission = {:.2}", rng.gen_range(100.0..1000.0)),
        format!("name LIKE '{}%'", ["A", "B", "C"][rng.gen_range(0..3)]),
        format!("last_update_by = '{}'", ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis"][rng.gen_range(0..5)]),
        format!("created_by = '{}'", ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis"][rng.gen_range(0..5)]),
        format!(
            "event_date BETWEEN '{}' AND '{}' ",
            Local::now()
            .checked_sub_signed(chrono::Duration::days(rng.gen_range(0..7)))
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
            Local::now()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
        ),


    ];
    let condition_count = rng.gen_range(4..=8);
    let mut selected_conditions = Vec::new();
    for _ in 0..condition_count {
        selected_conditions.push(conditions[rng.gen_range(0..conditions.len())].clone());
    }
    format!("WHERE {}", selected_conditions.join(" AND "))
}
