open Types
open Pure
open Parsers

let status_filter = ref None
let origin_filter = ref None
let output_as_sqlite = ref false
let set_output_sqlite () = output_as_sqlite := true

let set_status s = status_filter := Some s
let set_origin o = origin_filter := Some o

let () =
  let speclist = [
    ("--status", Arg.String set_status, "Filter orders by status (Pending, Complete, Cancelled)");
    ("--origin", Arg.String set_origin, "Filter orders by origin (P, O)");
    ("--sqlite", Arg.Unit set_output_sqlite, "Save output as a SQLite database")
  ] in
  Arg.parse speclist (fun _ -> ()) "Usage: etl_project [options]";

  let order_filename = "data/order.csv" in
  let order_item_filename = "data/order_item.csv" in
  
  let output_filename = "data/output" in
  let full_output_filename = if !output_as_sqlite then output_filename ^ ".db" else output_filename ^ ".csv" in

  let orders = process_order_csv order_filename in
  let orderItems = process_orderItem_csv order_item_filename in

  let filtered_orders = filter_orders orders !status_filter !origin_filter in
  let filtered_order_ids = List.map (fun order -> order.id) filtered_orders in
  let filtered_orderItems = List.filter (fun oi -> List.mem oi.order_id filtered_order_ids) orderItems in
  
  let aggregated = aggregate_order_items filtered_orderItems in
  if !output_as_sqlite then begin
    let db = Sqlite3.db_open full_output_filename in
    
    (* Create a table to store the aggregated order summary if it doesn't exist *)
    let create_table_sql = "CREATE TABLE IF NOT EXISTS order_summary (order_id INTEGER, total_amount REAL, total_taxes REAL);" in
    (match Sqlite3.exec db create_table_sql with
     | Sqlite3.Rc.OK -> ()
     | rc -> Printf.printf "Error creating table: %s\n" (Sqlite3.Rc.to_string rc));
    
    (* Insert each aggregated row into the database *)
    List.iter (fun (order_id, total_amount, total_taxes) ->
      let insert_sql = Printf.sprintf "INSERT INTO order_summary VALUES (%d, %.2f, %.2f);" order_id total_amount total_taxes in
      (match Sqlite3.exec db insert_sql with
       | Sqlite3.Rc.OK -> ()
       | rc -> Printf.printf "Error inserting row: %s\n" (Sqlite3.Rc.to_string rc))
    ) aggregated;
    
    Sqlite3.db_close db |> ignore;
    Printf.printf "\nOutput written to SQLite database at %s\n" full_output_filename
  end else begin
    let csv_rows =
      ("order_id" :: "total_amount" :: "total_taxes" :: []) ::
      List.map (fun (order_id, total_amount, total_taxes) ->
        [string_of_int order_id; Printf.sprintf "%.2f" total_amount; Printf.sprintf "%.2f" total_taxes]
      ) aggregated
    in
    Csv.save full_output_filename csv_rows;
    Printf.printf "\nOutput written to %s\n" full_output_filename
  end