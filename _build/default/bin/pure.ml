(* pure.ml *)
(* This module contains pure functions that do not produce side effects *)

open Types

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