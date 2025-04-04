(* parsers.ml *)
(** 
 * This module provides parsing functionality for the ETL project.
 * It contains functions to parse and validate different data formats,
 * convert between representations, and prepare data for the transformation
 * stage of the ETL pipeline.
 *  
 * The parsers implemented here handle various input formats and produce
 * structured data suitable for further processing in the ETL workflow.
*)

let ( let* ) = Result.bind

open Types
 
(** 
  Parses a string into an order or order item ID.
  @param s The string to parse
  @return Ok with the parsed integer ID, or Error with an error message
*)
let parser_id s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid id: %s" s)
 
(** 
  Parses a string into a client ID.
  @param s The string to parse
  @return Ok with the parsed integer client ID, or Error with an error message
*)
let parser_client_id s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid client_id: %s" s)
 
(** 
  Parses a string into a product ID.
  @param s The string to parse
  @return Ok with the parsed integer product ID, or Error with an error message
*)
let parser_product_id s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid product_id: %s" s)
 
(** 
  Parses an ISO format date string (YYYY-MM-DDThh:mm:ss) into an order_date record.
  @param s The date string to parse
  @return Ok with the structured order_date record, or Error with an error message
*)
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
 
(** 
  Parses a string into an order status enum value.
  @param s The string to parse ("Pending", "Complete", or "Cancelled")
  @return Ok with the corresponding status variant, or Error with an error message
*)
let parser_status = function
  | "Pending" -> Ok Pending
  | "Complete" -> Ok Complete
  | "Cancelled" -> Ok Cancelled
  | s -> Error (Printf.sprintf "Invalid status: %s" s)
 
(** 
  Parses a string into an order origin enum value.
  @param s The string to parse ("P" or "O")
  @return Ok with the corresponding origin variant, or Error with an error message
*)
let parser_origin = function
  | "P" -> Ok P
  | "O" -> Ok O
  | s -> Error (Printf.sprintf "Invalid origin: %s" s)
 
(** 
  Parses a string into an order item quantity.
  @param s The string to parse
  @return Ok with the parsed integer quantity, or Error with an error message
*)
let parser_quantity s =
  try Ok (int_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid quantity: %s" s)
 
(** 
  Parses a string into an order item price.
  @param s The string to parse
  @return Ok with the parsed float price, or Error with an error message
*)
let parser_price s =
  try Ok (float_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid price: %s" s)
 
(** 
  Parses a string into an order item tax rate.
  @param s The string to parse
  @return Ok with the parsed float tax rate, or Error with an error message
*)
let parser_tax s =
  try Ok (float_of_string s) with
  | Failure _ -> Error (Printf.sprintf "Invalid tax: %s" s)
 
(** 
  Parses an array of strings representing a CSV row into an order record.
  @param row Array of strings with fields [id; client_id; date; status; origin]
  @return Ok with the structured order record, or Error with an error message
*)
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
 
(** 
  Parses an array of strings representing a CSV row into an orderItem record.
  @param row Array of strings with fields [order_id; product_id; quantity; price; tax]
  @return Ok with the structured orderItem record, or Error with an error message
*)
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