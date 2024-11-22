use chrono::{Local};
use rand::Rng;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use regex::Regex;

// 定義 SQL 操作類型
const STATUS: [&str; 2] = ["SUCCESS", "FAILURE"];

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let default_entries = 300;
    let num_entries = if let Some(pos) = args.iter().position(|x| x == "-r") {
        args.get(pos + 1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(default_entries)
    } else {
        default_entries
    };

    let mut file = File::create("sql_logs.tsv")?;
    let mut rng = rand::thread_rng();
    // 寫入 TSV 標題
    writeln!(
        file,
        "conn_hash\tstmt_id\texec_id\texec_time\tsql_type\texe_status\tdb_ip\tclient_ip\tclient_host\tapp_name\tdb_user\tsql_hash\tfrom_tbs\tselect_cols\tsql_stmt\tstmt_bind_vars"
    )?;
    // 隨機產生指定筆數的 SQL 日誌資料
    for i in 0..num_entries {
        if i % 1000 == 0 {
            eprint!("\rGenerated {} entries...", i);
        }
        let conn_hash = format!("conn_{}", rng.gen::<u64>());
        let stmt_id = rng.gen_range(1..=100);
        let exec_id = rng.gen_range(1..=1000);
        let exec_time = Local::now()
            .checked_sub_signed(chrono::Duration::days(rng.gen_range(0..7)))
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let sql_type = match rng.gen_range(0..100) {
            0..=35 => "INSERT",
            36..=45 => "UPDATE",
            46..=50 => "DELETE",
            51..=53 => "ALTER",
            _ => "SELECT",
        };
        let exe_status = STATUS[rng.gen_range(0..STATUS.len())];
        let db_ip = format!(
            "192.168.{}.{}",
            rng.gen_range(0..255),
            rng.gen_range(0..255)
        );
        let client_ip = format!("10.0.{}.{}", rng.gen_range(0..255), rng.gen_range(0..255));
        let client_host = ["ERP_USER1", "ERP_USER2", "ERP_USER3", "ERP_USER4", "ERP_USER5"][rng.gen_range(0..5)].to_string();
        let app_name = match rng.gen_range(0..100) {
            0..=50 => "ERP",
            51..=80 => "WEB App",
            81..=85 => "SQL Developer",
            86..=90 => "Toad",
            91..=95 => "PL/SQL Developer",
            _ => "SQL*Plus",
        };
        let db_user = ["SYS", "SYSTEM", "HR", "SCOTT", "OE", "SH", "PM", "IX", "APEX_040000", "ANONYMOUS"][rng.gen_range(0..10)].to_string();
        let sql_hash = format!("hash_{}", rng.gen::<u64>());
        let table_names = ["users", "orders", "products", "departments", "employees", "salaries", "projects", "tasks", "events", "logs"];
        let from_tbs = table_names[rng.gen_range(0..table_names.len())];
        let select_cols = generate_select_cols(sql_type);
        let sql_stmt = generate_sql_stmt(sql_type, from_tbs);
        let bind_vars_example = extract_where_values(&sql_stmt);

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
            bind_vars_example
        )?;
    }

    println!("SQL logs generated and saved to sql_logs.tsv");
    Ok(())
}

// 隨機生成 SELECT 列名稱
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



/// Extract numbers and text values from the WHERE clause in a SQL statement.
///
/// # Arguments
/// * `sql` - A SQL statement containing a WHERE clause.
///
/// # Returns
/// A String containing extracted numbers and text values.
fn extract_where_values(sql: &str) -> String {
    // Define regex for numbers and text values
    let re = Regex::new(r"(?i)\bWHERE\b(.*)").unwrap(); // Match WHERE clause
    if let Some(caps) = re.captures(sql) {
        // Extract the WHERE clause
        let where_clause = caps.get(1).unwrap().as_str();

        // Define regex for numbers and text inside single quotes
        let value_re = Regex::new(r"[-+]?\d+(\.\d+)?|'[^']*'").unwrap();

        // Find all matches
        let mut values = Vec::new();
        for cap in value_re.captures_iter(where_clause) {
            values.push(cap[0].to_string());
        }

        // Join extracted values into a single string
        return values.join(", ");
    }

    // If no WHERE clause is found, return an empty string
    String::new()
}



// 隨機生成 SQL 語句
fn generate_sql_stmt(sql_type: &str, table: &str) -> String {
    let mut rng = rand::thread_rng();
    match sql_type {
        "SELECT" => {
            let where_clause = generate_where_clause();
            format!(
                "SELECT {} FROM {} {}",
                generate_select_cols("SELECT"),
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
