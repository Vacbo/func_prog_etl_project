(* types.ml *)
(* This module contains the types used in the application *)

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