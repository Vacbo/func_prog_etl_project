(* pure.ml *)
(* This module contains pure functions that do not produce side effects *)

open Types

(** 
  Aggregates order items by order ID to compute total amounts and taxes.
  @param orderItems The list of order items to aggregate
  @return List of tuples (order_id, total_amount, total_tax) representing the aggregated data
*)
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

(** 
  Filters orders based on optional status and origin criteria.
  @param orders The list of orders to filter
  @param status_opt Optional status filter as string ("Pending", "Complete", "Cancelled")
  @param origin_opt Optional origin filter as string ("P", "O")
  @return Filtered list of orders that match the given criteria
*)
let filter_orders orders status_opt origin_opt =
  List.filter (fun order ->
    let status_ok = match status_opt with
      | None -> true
      | Some s -> (
          match (s, order.status) with
          | ("Pending", Pending) -> true
          | ("Complete", Complete) -> true
          | ("Cancelled", Cancelled) -> true
          | _ -> false
        )
    in
    let origin_ok = match origin_opt with
      | None -> true
      | Some o -> (
          match (o, order.origin) with
          | ("P", P) -> true
          | ("O", O) -> true
          | _ -> false
        )
    in
    status_ok && origin_ok
  ) orders

(** 
  Performs an inner join between orders and orderItems with optional filtering.
  @param orders The list of orders
  @param orderItems The list of order items
  @param status_filter Optional filter for order status
  @param origin_filter Optional filter for order origin
  @return The filtered list of order items that match the join and filter conditions
*)
 let join_orders_and_items orders orderItems ?status_filter ?origin_filter () =
  List.filter_map (fun oi ->
    match List.find_opt (fun order -> order.id = oi.order_id) orders with
    | Some order ->
        let status_ok = match status_filter with
          | None -> true
          | Some s -> (
              match (s, order.status) with
              | ("Pending", Pending) -> true
              | ("Complete", Complete) -> true
              | ("Cancelled", Cancelled) -> true
              | _ -> false
            )
        in
        let origin_ok = match origin_filter with
          | None -> true
          | Some o -> (
              match (o, order.origin) with
              | ("P", P) -> true
              | ("O", O) -> true
              | _ -> false
            )
        in
        if status_ok && origin_ok then Some oi else None
    | None -> None
  ) orderItems

(** 
  Extracts year and month from an order_date.
  @param order_date The date to extract from
  @return A tuple of (year, month)
*)
 let get_year_month order_date =
  (order_date.year, order_date.month)

(** 
  Aggregates order items by month and year, calculating average revenue and taxes.
  @param orders The list of orders
  @param order_items The list of order items (already filtered if needed)
  @return List of ((year, month), avg_amount, avg_tax) tuples sorted chronologically
*)
let aggregate_by_month_year orders order_items =
  (* First, pair each order item with its order *)
  let items_with_orders = 
    List.filter_map (fun item ->
      match List.find_opt (fun order -> order.id = item.order_id) orders with
      | Some order -> Some (item, order)
      | None -> None
    ) order_items
  in

  (* Group by year and month *)
  let grouped = Hashtbl.create 50 in
  List.iter (fun (item, order) ->
    let year_month = get_year_month order.order_date in
    let amount = item.price *. float_of_int item.quantity in
    let tax = amount *. item.tax in
    
    if Hashtbl.mem grouped year_month then
      let (count, sum_amount, sum_tax) = Hashtbl.find grouped year_month in
      Hashtbl.replace grouped year_month (count + 1, sum_amount +. amount, sum_tax +. tax)
    else
      Hashtbl.add grouped year_month (1, amount, tax)
  ) items_with_orders;

  (* Convert hashtable to list and calculate averages *)
  Hashtbl.fold (fun (year, month) (count, sum_amount, sum_tax) acc ->
    let avg_amount = sum_amount /. float_of_int count in
    let avg_tax = sum_tax /. float_of_int count in
    ((year, month), avg_amount, avg_tax) :: acc
  ) grouped []
  |> List.sort (fun ((y1, m1), _, _) ((y2, m2), _, _) ->
    if y1 <> y2 then compare y1 y2 else compare m1 m2)