open Pure
open IoHelper

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

  (* Load and process data *)
  let orders = process_order_csv order_filename in
  let orderItems = process_orderItem_csv order_item_filename in
  let filtered_orderItems = join_orders_and_items orders orderItems
    ?status_filter:!status_filter
    ?origin_filter:!origin_filter
    () in
  let aggregated = aggregate_order_items filtered_orderItems in
  
  (* Write output *)
  let _ = write_output output_filename !output_as_sqlite aggregated in
  ()