use chrono::{Local, NaiveDateTime};
use rand::Rng;
use std::error::Error;
use std::fs::File;
use std::io::Write;

// 定義 SQL 操作類型
const SQL_TYPES: [&str; 4] = ["SELECT", "INSERT", "UPDATE", "DELETE"];
const STATUS: [&str; 2] = ["SUCCESS", "FAILURE"];

fn main() -> Result<(), Box<dyn Error>> {
    let mut file = File::create("sql_logs.tsv")?;
    let mut rng = rand::thread_rng();

    // 隨機產生 100 筆 SQL 日誌資料
    for _ in 0..100 {
        let conn_hash = format!("conn_{}", rng.gen::<u64>());
        let stmt_id = rng.gen_range(1..=1000);
        let exec_id = rng.gen_range(1..=10);
        let exec_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let sql_type = SQL_TYPES[rng.gen_range(0..SQL_TYPES.len())];
        let exe_status = STATUS[rng.gen_range(0..STATUS.len())];
        let db_ip = format!(
            "192.168.{}.{}",
            rng.gen_range(0..255),
            rng.gen_range(0..255)
        );
        let client_ip = format!("10.0.{}.{}", rng.gen_range(0..255), rng.gen_range(0..255));
        let client_host = format!("host_{}", rng.gen_range(1..100));
        let app_name = format!("app_{}", rng.gen_range(1..5));
        let db_user = format!("user_{}", rng.gen_range(1..10));
        let sql_hash = format!("hash_{}", rng.gen::<u64>());
        let from_tbs = format!("tbs_{}", rng.gen_range(1..5));
        let select_cols = generate_select_cols(sql_type);
        let sql_stmt = generate_sql_stmt(sql_type);

        // 寫入 TSV 格式的資料
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            conn_hash,
            stmt_id,
            exec_id,
            exec_time,
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
            "bind_vars_example"
        )?;
    }

    println!("SQL logs generated and saved to sql_logs.tsv");
    Ok(())
}

// 隨機生成 SELECT 列名稱
fn generate_select_cols(sql_type: &str) -> String {
    if sql_type == "SELECT" {
        let columns = ["id", "name", "age", "salary", "department"];
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

// 隨機生成 SQL 語句
fn generate_sql_stmt(sql_type: &str) -> String {
    let mut rng = rand::thread_rng();
    match sql_type {
        "SELECT" => {
            let table = format!("table_{}", rng.gen_range(1..5));
            let where_clause = generate_where_clause();
            format!(
                "SELECT {} FROM {} {}",
                generate_select_cols("SELECT"),
                table,
                where_clause
            )
        }
        "INSERT" => {
            let table = format!("table_{}", rng.gen_range(1..5));
            let values = format!(
                "({}, '{}', {}, {})",
                rng.gen_range(1..100),
                "John Doe",
                rng.gen_range(20..50),
                rng.gen_range(3000..10000)
            );
            format!(
                "INSERT INTO {} (id, name, age, salary) VALUES {}",
                table, values
            )
        }
        "UPDATE" => {
            let table = format!("table_{}", rng.gen_range(1..5));
            let set_clause = format!("SET salary = {}", rng.gen_range(3000..10000));
            let where_clause = generate_where_clause();
            format!("UPDATE {} {} {}", table, set_clause, where_clause)
        }
        "DELETE" => {
            let table = format!("table_{}", rng.gen_range(1..5));
            let where_clause = generate_where_clause();
            format!("DELETE FROM {} {}", table, where_clause)
        }
        _ => String::new(),
    }
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
        format!("name LIKE '{}%'", ["A", "B", "C"][rng.gen_range(0..3)]),
    ];
    let condition_count = rng.gen_range(1..=3);
    let mut selected_conditions = Vec::new();
    for _ in 0..condition_count {
        selected_conditions.push(conditions[rng.gen_range(0..conditions.len())].clone());
    }
    format!("WHERE {}", selected_conditions.join(" AND "))
}
