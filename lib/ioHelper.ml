(** 
 * IoHelper - Module for CSV file processing and data export operations
 * This module provides functionality for reading CSV files, parsing data,
 * and writing processed data to various output formats.
*)

open Parsers

(** 
  Reads and parses order data from a CSV file.
  @param file Path to the CSV file containing order data
  @return List of successfully parsed order records
*)
let process_order_csv file =
  try
    let rows = Csv.load file in
    match rows with
    | _header :: data_rows ->
        (* Skip header row and parse data *)
        let results = List.map parse_order_row data_rows in
        let orders, errors = 
          List.partition_map (function 
            | Ok order -> Left order 
            | Error msg -> Right msg) results
        in
        (* Report parsing results *)
        Printf.printf "Successfully parsed %d orders\n" (List.length orders);
        if List.length errors > 0 then begin
          Printf.printf "Found %d errors:\n" (List.length errors);
          List.iter (Printf.printf "  - %s\n") errors
        end;
        orders
    | [] -> 
        Printf.printf "Empty CSV file\n";
        []
  with
  | Csv.Failure (row, col, msg) -> 
      Printf.printf "CSV parsing error at row %d, column %d: %s\n" row col msg;
      []
  | Sys_error msg ->
      Printf.printf "File error: %s\n" msg;
      []
 
(** 
  Reads and parses order item data from a CSV file.
  @param file Path to the CSV file containing order item data
  @return List of successfully parsed orderItem records
*)
let process_orderItem_csv file =
  try
    let rows = Csv.load file in
    match rows with
    | _header :: data_rows ->
        (* Skip header row and parse data *)
        let results = List.map parse_orderItem_row data_rows in
        let orderItems, errors = 
          List.partition_map (function 
            | Ok orderItem -> Left orderItem 
            | Error msg -> Right msg) results
        in
        (* Report parsing results *)
        Printf.printf "Successfully parsed %d orderItems\n" (List.length orderItems);
        if List.length errors > 0 then begin
          Printf.printf "Found %d errors:\n" (List.length errors);
          List.iter (Printf.printf "  - %s\n") errors
        end;
        orderItems
    | [] -> 
        Printf.printf "Empty CSV file\n";
        []
  with
  | Csv.Failure (row, col, msg) -> 
      Printf.printf "CSV parsing error at row %d, column %d: %s\n" row col msg;
      []
  | Sys_error msg ->
      Printf.printf "File error: %s\n" msg;
      []

(** 
  Downloads a file from a URL and saves it to a temporary file.
  Requires the cohttp, cohttp-lwt-unix, and lwt packages.

  @param url The URL to download from
  @return Result containing either the temporary file path or an error message
*)
let download_file url =
  try
    (* Create a temporary file *)
    let temp_file = Filename.temp_file "etl_download_" ".csv" in
    
    (* Set up the command using curl (available on most systems) *)
    let cmd = Printf.sprintf "curl -s -o %s %s" (Filename.quote temp_file) (Filename.quote url) in
    
    (* Execute the command *)
    let status = Sys.command cmd in
    
    if status = 0 then
      (* Success - return the temp file path *)
      Ok temp_file
    else
      (* Curl failed *)
      Error (Printf.sprintf "Failed to download from %s (exit code: %d)" url status)
  with e ->
    (* Handle any exceptions *)
    Error (Printf.sprintf "Exception while downloading: %s" (Printexc.to_string e))
 
(** 
  Writes aggregated data to a SQLite database file.
  Creates a table named 'order_summary' with columns for order_id, 
  total_amount, and total_taxes.

  @param filename The path to the output database file
  @param aggregated The aggregated order data as a list of (order_id, total_amount, total_taxes) tuples
*)
let write_to_sqlite filename aggregated =
  let db = Sqlite3.db_open filename in
  
  (* Create table *)
  let create_table_sql = "CREATE TABLE IF NOT EXISTS order_summary (order_id INTEGER, total_amount REAL, total_taxes REAL);" in
  (match Sqlite3.exec db create_table_sql with
  | Sqlite3.Rc.OK -> ()
  | rc -> Printf.printf "Error creating table: %s\n" (Sqlite3.Rc.to_string rc));
  
  (* Insert data *)
  List.iter (fun (order_id, total_amount, total_taxes) ->
    let insert_sql = Printf.sprintf "INSERT INTO order_summary VALUES (%d, %.2f, %.2f);" order_id total_amount total_taxes in
    (match Sqlite3.exec db insert_sql with
    | Sqlite3.Rc.OK -> ()
    | rc -> Printf.printf "Error inserting row: %s\n" (Sqlite3.Rc.to_string rc))
  ) aggregated;
  
  Sqlite3.db_close db |> ignore;
  Printf.printf "\nOutput written to SQLite database at %s\n" filename
 
(** 
  Writes aggregated data to a CSV file.
  Creates a CSV with header row "order_id,total_amount,total_taxes" and
  formats all numerical values appropriately.

  @param filename The path to the output CSV file
  @param aggregated The aggregated order data as a list of (order_id, total_amount, total_taxes) tuples
*)
let write_to_csv filename aggregated =
  let csv_rows =
    ("order_id" :: "total_amount" :: "total_taxes" :: []) ::
    List.map (fun (order_id, total_amount, total_taxes) ->
      [string_of_int order_id; Printf.sprintf "%.2f" total_amount; Printf.sprintf "%.2f" total_taxes]
    ) aggregated
  in
  Csv.save filename csv_rows;
  Printf.printf "\nOutput written to %s\n" filename
 
(** 
  Writes data to either SQLite or CSV based on the settings.
  Appends the appropriate file extension (.db or .csv) to the base filename.

  @param filename The base output filename (without extension)
  @param use_sqlite Whether to use SQLite format (true) or CSV format (false)
  @param aggregated The aggregated order data to write
  @return The full filename with appropriate extension
*)
let write_output filename use_sqlite aggregated =
  let full_filename = if use_sqlite then filename ^ ".db" else filename ^ ".csv" in
  if use_sqlite then
    write_to_sqlite full_filename aggregated
  else
    write_to_csv full_filename aggregated;
  full_filename

(** 
  Writes monthly average data to a CSV file.
  @param filename The path to the output CSV file
  @param monthly_data The list of monthly averages as ((year, month), avg_amount, avg_tax) tuples
*)
 let write_monthly_averages_to_csv filename monthly_data =
  let csv_rows =
    ("year" :: "month" :: "avg_amount" :: "avg_tax" :: []) ::
    List.map (fun ((year, month), avg_amount, avg_tax) ->
      [string_of_int year; 
       string_of_int month; 
       Printf.sprintf "%.2f" avg_amount; 
       Printf.sprintf "%.2f" avg_tax]
    ) monthly_data
  in
  Csv.save filename csv_rows;
  Printf.printf "Monthly averages written to %s\n" filename

(** 
  Writes monthly average data to a SQLite database file.
  @param filename The path to the output database file
  @param monthly_data The list of monthly averages as ((year, month), avg_amount, avg_tax) tuples
*)
let write_monthly_averages_to_sqlite filename monthly_data =
  let db = Sqlite3.db_open filename in
  
  (* Create table *)
  let create_table_sql = 
    "CREATE TABLE IF NOT EXISTS monthly_averages (year INTEGER, month INTEGER, avg_amount REAL, avg_tax REAL);" in
  (match Sqlite3.exec db create_table_sql with
   | Sqlite3.Rc.OK -> ()
   | rc -> Printf.printf "Error creating monthly averages table: %s\n" (Sqlite3.Rc.to_string rc));
  
  (* Insert data *)
  List.iter (fun ((year, month), avg_amount, avg_tax) ->
    let insert_sql = Printf.sprintf 
      "INSERT INTO monthly_averages VALUES (%d, %d, %.2f, %.2f);" 
      year month avg_amount avg_tax in
    (match Sqlite3.exec db insert_sql with
     | Sqlite3.Rc.OK -> ()
     | rc -> Printf.printf "Error inserting monthly average row: %s\n" (Sqlite3.Rc.to_string rc))
  ) monthly_data;
  
  Sqlite3.db_close db |> ignore;
  Printf.printf "Monthly averages written to SQLite database at %s\n" filename

(** 
  Writes monthly average data to either SQLite or CSV based on the settings.
  @param base_filename The base output filename (without extension)
  @param use_sqlite Whether to use SQLite format (true) or CSV format (false)
  @param monthly_data The monthly average data to write
*)
let write_monthly_averages base_filename use_sqlite monthly_data =
  let filename = base_filename ^ "_monthly" in
  if use_sqlite then
    write_monthly_averages_to_sqlite (filename ^ ".db") monthly_data
  else
    write_monthly_averages_to_csv (filename ^ ".csv") monthly_data