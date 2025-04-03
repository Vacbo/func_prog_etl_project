

open Types
open Pure
open Parsers


(* csv parsers *)


let status_filter = ref None
let origin_filter = ref None

let set_status s = status_filter := Some s
let set_origin o = origin_filter := Some o

(* Main entry point *)
let () =
  let speclist = [
    ("--status", Arg.String set_status, "Filter orders by status (Pending, Complete, Cancelled)");
    ("--origin", Arg.String set_origin, "Filter orders by origin (P, O)");
  ] in
  Arg.parse speclist (fun _ -> ()) "Usage: etl_project [options]";

  let order_filename = "data/order.csv" in
  let order_item_filename = "data/order_item.csv" in
  let orders = process_order_csv order_filename in
  let orderItems = process_orderItem_csv order_item_filename in
  let filtered_orders = filter_orders orders !status_filter !origin_filter in
  let filtered_order_ids = List.map (fun order -> order.id) filtered_orders in
  let filtered_orderItems = List.filter (fun oi -> List.mem oi.order_id filtered_order_ids) orderItems in
  
  (* Aggregate order items using the pure function from Pure module *)
  let output_filename = "data/output.csv" in
  let aggregated = aggregate_order_items filtered_orderItems in
  let csv_rows =
    ("order_id" :: "total_ amount" :: "total_taxes" :: []) ::
    List.map (fun (order_id, total_amount, total_taxes) ->
      [string_of_int order_id; Printf.sprintf "%.2f" total_amount; Printf.sprintf "%.2f" total_taxes]
    ) aggregated
  in
  Csv.save output_filename csv_rows;
  Printf.printf "\nOutput written to %s\n" output_filename;