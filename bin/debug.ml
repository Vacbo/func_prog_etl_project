(* debug.ml *)
(* This module contains functions for debugging and printing data structures *)

open Types

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