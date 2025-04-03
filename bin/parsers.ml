(* parsers.ml *)
(* This module provides parsing functionality for the ETL project.
  It contains functions to parse and validate different data formats,
  convert between representations, and prepare data for the transformation
  stage of the ETL pipeline.
  
  The parsers implemented here handle various input formats and produce
  structured data suitable for further processing in the ETL workflow.
*)

let ( let* ) = Result.bind

open Types

let parser_id s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid id: %s" s)

let parser_client_id s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid client_id: %s" s)

let parser_product_id s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid product_id: %s" s)

let parser_order_date s =
  try
    match String.split_on_char 'T' s with
    | [date_part; time_part] -> (
        let date_components = String.split_on_char '-' date_part in
        let time_components = String.split_on_char ':' time_part in
        match (date_components, time_components) with
        | ([year; month; day], [hour; minute; second]) ->
            Ok {
              year = int_of_string year;
              month = int_of_string month;
              day = int_of_string day;
              hour = int_of_string hour;
              minute = int_of_string minute;
              second = int_of_string second;
            }
        | _ -> Error (Printf.sprintf "Invalid ISO format components in: %s" s)
      )
    | _ -> Error (Printf.sprintf "Invalid ISO format (missing T separator): %s" s)
  with Failure _ -> Error (Printf.sprintf "Number conversion error in date: %s" s)

let parser_status = function
  | "Pending" -> Ok Pending
  | "Complete" -> Ok Complete
  | "Cancelled" -> Ok Cancelled
  | s -> Error (Printf.sprintf "Invalid status: %s" s)

let parser_origin = function
  | "P" -> Ok P
  | "O" -> Ok O
  | s -> Error (Printf.sprintf "Invalid origin: %s" s)

let parser_quantity s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid quantity: %s" s)

let parser_price s =
  try Ok (float_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid price: %s" s)

let parser_tax s =
  try Ok (float_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid tax: %s" s)

(* Parse a single CSV row into an order *)
let parse_order_row row =
  match row with
  | [id_str; client_id_str; date_str; status_str; origin_str] ->
      let* id = parser_id id_str in
      let* client_id = parser_client_id client_id_str in
      let* order_date = parser_order_date date_str in
      let* status = parser_status status_str in
      let* origin = parser_origin origin_str in
      Ok { id; client_id; order_date; status; origin }
  | _ -> Error "Row doesn't have the expected number of columns"

let parse_orderItem_row row =
  match row with
  | [order_id_str; product_id_str; quantity_str; price_str; tax_str] ->
      let* order_id = parser_id order_id_str in
      let* product_id = parser_product_id product_id_str in
      let* quantity = parser_quantity quantity_str in
      let* price = parser_price price_str in
      let* tax = parser_tax tax_str in
      Ok { order_id; product_id; quantity; price; tax }
  | _ -> Error "Row doesn't have the expected number of columns"

(* Partition a list into two lists based on a predicate *)

(* Process the entire CSV file *)
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