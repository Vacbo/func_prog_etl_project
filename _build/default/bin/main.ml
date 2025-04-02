let ( let* ) = Result.bind

(* csv custom types *)
type id = int

type client_id = int

type order_date = {
  year: int;
  month: int;
  day: int;
  hour: int;
  minute: int;
  second: int;
}

type status = Pending | Complete | Cancelled

type origin = P | O

type order = {
  id: id;
  client_id: client_id;
  order_date: order_date;
  status: status;
  origin: origin;
}

type order_id = int

type product_id = int

type quantity = int

type price = float

type tax = float (* percentage *)

type orderItem = {
  order_id: order_id;
  product_id: product_id;
  quantity: quantity;
  price: price;
  tax: tax;
}

(* csv parsers *)
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

(* Debug print for an order *)
let print_order order =
  Printf.printf "Order ID: %d\n" order.id;
  Printf.printf "  Client: %d\n" order.client_id;
  Printf.printf "  Date: %d-%02d-%02d %02d:%02d:%02d\n"
    order.order_date.year
    order.order_date.month
    order.order_date.day
    order.order_date.hour
    order.order_date.minute
    order.order_date.second;
  Printf.printf "  Status: %s\n" 
    (match order.status with
      | Pending -> "Pending"
      | Complete -> "Complete"
      | Cancelled -> "Cancelled");
  Printf.printf "  Origin: %s\n" 
    (match order.origin with
      | P -> "P"
      | O -> "O");
  print_newline ()

(* Debug print for an orderItem *)
let print_orderItem orderItem =
  Printf.printf "Order ID: %d\n" orderItem.order_id;
  Printf.printf "  Product: %d\n" orderItem.product_id;
  Printf.printf "  Quantity: %d\n" orderItem.quantity;
  Printf.printf "  Price: %.2f\n" orderItem.price;
  Printf.printf "  Tax: %.2f\n" orderItem.tax;
  print_newline ()


let aggregate_order_items orderItems =
  let table = Hashtbl.create 10 in
  List.iter (fun oi -> 
    let amnt = float_of_int oi.quantity *. oi.price in   
    let tax_amt = amnt *. oi.tax in
    if Hashtbl.mem table oi.order_id then
      let (old_amt, old_tax) = Hashtbl.find table oi.order_id in
      Hashtbl.replace table oi.order_id (old_amt +. amnt, old_tax +. tax_amt)
    else
      Hashtbl.add table oi.order_id (amnt, tax_amt)
  ) orderItems;
  Hashtbl.fold (fun order_id (amt, tax_amt) acc -> 
    (order_id, amt, tax_amt) :: acc
  ) table []

(* Main entry point *)
let () =
  let order_filename = "data/order.csv" in
  let order_item_filename = "data/order_item.csv" in
  let orders = process_order_csv order_filename in
  Printf.printf "Loaded %d orders\n" (List.length orders);
  let orderItems = process_orderItem_csv order_item_filename in
  Printf.printf "Loaded %d orderItems\n" (List.length orderItems);

  (* Debug print for the first few orders *)
  let sample_size = min 5 (List.length orders) in
  Printf.printf "\nPrinting first %d orders:\n" sample_size;
  List.take sample_size orders |> List.iter print_order;
  
  (* Debug print for the first few orderItems *)
  let sample_size = min 5 (List.length orderItems) in
  Printf.printf "\nPrinting first %d orderItems:\n" sample_size;
  List.take sample_size orderItems |> List.iter print_orderItem;

  (* Aggregate order items *)
  let output_filename = "data/output.csv" in
  let aggregated = aggregate_order_items orderItems in
  let csv_rows =
    ("order_id" :: "total_ amount" :: "total_taxes" :: []) ::
    List.map (fun (order_id, total_amount, total_taxes) ->
      [string_of_int order_id; Printf.sprintf "%.2f" total_amount; Printf.sprintf "%.2f" total_taxes]
    ) aggregated
  in
  Csv.save output_filename csv_rows;
  Printf.printf "\nOutput written to %s\n" output_filename;

