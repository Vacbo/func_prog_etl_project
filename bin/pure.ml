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