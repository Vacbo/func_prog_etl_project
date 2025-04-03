open Pure
open IoHelper

let status_filter = ref None
let origin_filter = ref None
let output_as_sqlite = ref false
let order_url = ref None
let order_item_url = ref None

let set_output_sqlite () = output_as_sqlite := true
let set_status s = status_filter := Some s
let set_origin o = origin_filter := Some o
let set_order_url url = order_url := Some url
let set_order_item_url url = order_item_url := Some url

let () =
  let speclist = [
    ("--status", Arg.String set_status, "Filter orders by status (Pending, Complete, Cancelled)");
    ("--origin", Arg.String set_origin, "Filter orders by origin (P, O)");
    ("--sqlite", Arg.Unit set_output_sqlite, "Save output as a SQLite database");
    ("--order-url", Arg.String set_order_url, "URL to download order CSV data from");
    ("--order-item-url", Arg.String set_order_item_url, "URL to download order item CSV data from")
  ] in
  Arg.parse speclist (fun _ -> ()) "Usage: etl_project [options]";

  (* Determine input file sources *)
  let order_source = match !order_url with
    | Some url -> 
      Printf.printf "Downloading orders from: %s\n" url;
      download_file url
    | None -> 
      let file = "data/order.csv" in
      if Sys.file_exists file then Ok file
      else Error (Printf.sprintf "Local file not found: %s" file)
  in
  
  let order_item_source = match !order_item_url with
    | Some url -> 
      Printf.printf "Downloading order items from: %s\n" url;
      download_file url
    | None -> 
      let file = "data/order_item.csv" in
      if Sys.file_exists file then Ok file
      else Error (Printf.sprintf "Local file not found: %s" file)
  in
  
  (* Process the files if available *)
  match order_source, order_item_source with
  | Ok order_file, Ok order_item_file ->
    (* Load and process data *)
    let orders = process_order_csv order_file in
    let orderItems = process_orderItem_csv order_item_file in
    
    (* Clean up temporary files *)
    if !order_url <> None then Sys.remove order_file;
    if !order_item_url <> None then Sys.remove order_item_file;
    
    (* Continue with the rest of the processing *)
    let filtered_orderItems = join_orders_and_items orders orderItems
      ?status_filter:!status_filter
      ?origin_filter:!origin_filter
      () in
    let aggregated = aggregate_order_items filtered_orderItems in
      
    (* Write output *)
    let _ = write_output "data/output" !output_as_sqlite aggregated in
    ()
      
  | Error err1, Error err2 ->
    Printf.printf "Error with both input sources:\n";
    Printf.printf "  Orders: %s\n" err1;
    Printf.printf "  Order Items: %s\n" err2
      
  | Error err, _ ->
    Printf.printf "Error with order source: %s\n" err
      
  | _, Error err ->
    Printf.printf "Error with order item source: %s\n" err