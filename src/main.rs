use chrono::{Local, Duration};
use rand::Rng;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
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
    exec_time: chrono::DateTime<Local>,
    sql_stmt: String,
    sql_hash: String,
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

    // 決定要產生的連線數量（10 到 200 個）
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
                sql_stmt: String::new(),
                sql_hash: String::new(),
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
            conn_info.sql_stmt = format!("SELECT {} FROM {} WHERE id = ?", select_cols, from_tbs);
            conn_info.sql_hash = format!("hash_{}", rng.gen::<u64>());

            while conn_info.current_entries < num_entries {
                // 每產生一筆 SQL，消耗一個 exec_id
                conn_info.exec_id += 1;

                // 檢查 exec_id 是否達到 10
                if conn_info.exec_id > 10 {
                    conn_info.stmt_id += 1;
                    conn_info.exec_id = 1;
                }

                // 檢查 stmt_id 是否達到 800000
                if conn_info.stmt_id > 800000 {
                    // 廢棄 conn_hash，生成新的 conn_hash，重置 stmt_id 和 exec_id
                    conn_info.conn_hash = format!("conn_{}", rng.gen::<u64>());
                    conn_info.stmt_id = 1;
                    conn_info.exec_id = 1;
                    // 重置 exec_time
                    conn_info.exec_time = Local::now() - Duration::days(rng.gen_range(1..=3));

                    // 生成新的 sql_stmt 和 sql_hash
                    let from_tbs = table_names[rng.gen_range(0..table_names.len())];
                    conn_info.sql_stmt = format!("SELECT {} FROM {} WHERE id = ?", select_cols, from_tbs);
                    conn_info.sql_hash = format!("hash_{}", rng.gen::<u64>());
                }

                // 每次消耗都讓 exec_time 增加 1~30 秒
                let increment_seconds = rng.gen_range(1..=30);
                conn_info.exec_time =
                    conn_info.exec_time + Duration::seconds(increment_seconds as i64);

                // 生成其他欄位
                let exe_status = STATUS[rng.gen_range(0..STATUS.len())];
                let db_ip = format!(
                    "192.168.{}.{}",
                    rng.gen_range(0..255),
                    rng.gen_range(0..255)
                );
                let client_ip = format!(
                    "10.0.{}.{}",
                    rng.gen_range(0..255),
                    rng.gen_range(0..255)
                );
                let client_host = [
                    "ERP_USER1",
                    "ERP_USER2",
                    "ERP_USER3",
                    "ERP_USER4",
                    "ERP_USER5",
                ][rng.gen_range(0..5)]
                    .to_string();
                let app_name = match rng.gen_range(0..100) {
                    0..=50 => "ERP",
                    51..=80 => "WEB App",
                    81..=85 => "SQL Developer",
                    86..=90 => "Toad",
                    91..=95 => "PL/SQL Developer",
                    _ => "SQL*Plus",
                };
                let db_user = [
                    "SYS",
                    "SYSTEM",
                    "HR",
                    "SCOTT",
                    "OE",
                    "SH",
                    "PM",
                    "IX",
                    "APEX_040000",
                    "ANONYMOUS",
                ][rng.gen_range(0..10)]
                    .to_string();
                let bind_vars_example = rng.gen_range(1..=10000).to_string();

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
                        conn_info.sql_hash,
                        from_tbs,
                        select_cols,
                        conn_info.sql_stmt,
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
