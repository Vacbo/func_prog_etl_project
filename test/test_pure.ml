(* test_pure.ml *)
open OUnit2
open Types
open Pure

(** Test fixtures - Sample data for testing *)

(* Sample order dates *)
let date1 = { year = 2024; month = 1; day = 15; hour = 10; minute = 30; second = 0 }
let date2 = { year = 2024; month = 2; day = 20; hour = 14; minute = 45; second = 30 }
let date3 = { year = 2024; month = 2; day = 25; hour = 9; minute = 15; second = 45 }
let date4 = { year = 2024; month = 3; day = 5; hour = 16; minute = 20; second = 15 }

(* Sample orders with different statuses and origins *)
let orders = [
  { id = 1; client_id = 101; order_date = date1; status = Pending; origin = P };
  { id = 2; client_id = 102; order_date = date2; status = Complete; origin = O };
  { id = 3; client_id = 103; order_date = date3; status = Cancelled; origin = P };
  { id = 4; client_id = 104; order_date = date4; status = Complete; origin = O };
]

(* Sample order items with various quantities, prices, and taxes *)
let order_items = [
  { order_id = 1; product_id = 201; quantity = 2; price = 10.0; tax = 0.05 };
  { order_id = 1; product_id = 202; quantity = 1; price = 20.0; tax = 0.1 };
  { order_id = 2; product_id = 203; quantity = 3; price = 15.0; tax = 0.07 };
  { order_id = 3; product_id = 204; quantity = 1; price = 50.0; tax = 0.05 };
  { order_id = 4; product_id = 205; quantity = 2; price = 25.0; tax = 0.08 };
  { order_id = 5; product_id = 206; quantity = 1; price = 30.0; tax = 0.1 }; (* No matching order *)
]

(** Test functions *)

(* Test aggregate_order_items function *)
let test_aggregate_order_items _ =
  let result = aggregate_order_items order_items in
  (* Sort by order_id for consistent comparison *)
  let sorted_result = List.sort (fun (id1, _, _) (id2, _, _) -> compare id1 id2) result in
  
  (* Expected aggregated results: (order_id, total_amount, total_tax) *)
  let expected = [
    (1, 40.0, 3.0);    (* (2*10*0.05) + (1*20*0.1) = 1.0 + 2.0 = 3.0 *)
    (2, 45.0, 3.15);   (* 3*15 = 45, taxes = 45*0.07 = 3.15 *)
    (3, 50.0, 2.5);    (* 1*50 = 50, taxes = 50*0.05 = 2.5 *)
    (4, 50.0, 4.0);    (* 2*25 = 50, taxes = 50*0.08 = 4.0 *)
    (5, 30.0, 3.0);    (* 1*30 = 30, taxes = 30*0.1 = 3.0 *)
  ] |> List.sort (fun (id1, _, _) (id2, _, _) -> compare id1 id2) in
  
  (* Custom equality function for tuples with floats *)
  let tuples_approx_equal expected actual =
    List.length expected = List.length actual &&
    List.for_all2 (fun (id1, amt1, tax1) (id2, amt2, tax2) ->
      id1 = id2 && 
      abs_float (amt1 -. amt2) < 0.01 && 
      abs_float (tax1 -. tax2) < 0.01
    ) expected actual
  in
  
  (* Assert that the aggregation matches expected results *)
  assert_equal ~msg:"Aggregated order items should match expected results"
               ~cmp:tuples_approx_equal
               ~printer:(fun l -> String.concat "; " (List.map (fun (id, amt, tax) -> 
                  Printf.sprintf "(%d, %.2f, %.2f)" id amt tax) l))
               expected sorted_result

(* Test filter_orders function *)
let test_filter_orders_status _ =
  (* Test filtering by status *)
  let pending_orders = filter_orders orders (Some "Pending") None in
  let complete_orders = filter_orders orders (Some "Complete") None in
  let cancelled_orders = filter_orders orders (Some "Cancelled") None in
  
  assert_equal ~msg:"Should find 1 pending order" 1 (List.length pending_orders);
  assert_equal ~msg:"Should find 2 complete orders" 2 (List.length complete_orders);
  assert_equal ~msg:"Should find 1 cancelled order" 1 (List.length cancelled_orders);
  
  (* Verify order IDs for each status *)
  assert_equal ~msg:"Pending order should have ID 1" 1 (List.hd pending_orders).id;
  assert_bool "Complete orders should include order 2" 
    (List.exists (fun o -> o.id = 2) complete_orders);
  assert_bool "Complete orders should include order 4" 
    (List.exists (fun o -> o.id = 4) complete_orders);
  assert_equal ~msg:"Cancelled order should have ID 3" 3 (List.hd cancelled_orders).id

let test_filter_orders_origin _ =
  (* Test filtering by origin *)
  let p_orders = filter_orders orders None (Some "P") in
  let o_orders = filter_orders orders None (Some "O") in
  
  assert_equal ~msg:"Should find 2 P-origin orders" 2 (List.length p_orders);
  assert_equal ~msg:"Should find 2 O-origin orders" 2 (List.length o_orders);
  
  (* Verify order IDs for each origin *)
  assert_bool "P-origin orders should include order 1" 
    (List.exists (fun o -> o.id = 1) p_orders);
  assert_bool "P-origin orders should include order 3" 
    (List.exists (fun o -> o.id = 3) p_orders);
  assert_bool "O-origin orders should include order 2" 
    (List.exists (fun o -> o.id = 2) o_orders);
  assert_bool "O-origin orders should include order 4" 
    (List.exists (fun o -> o.id = 4) o_orders)

let test_filter_orders_combined _ =
  (* Test combined filtering (status AND origin) *)
  let complete_p_orders = filter_orders orders (Some "Complete") (Some "P") in
  let complete_o_orders = filter_orders orders (Some "Complete") (Some "O") in
  
  assert_equal ~msg:"Should find 0 complete P-origin orders" 0 (List.length complete_p_orders);
  assert_equal ~msg:"Should find 2 complete O-origin orders" 2 (List.length complete_o_orders)

(* Test join_orders_and_items function *)
let test_join_orders_and_items _ =
  (* Basic join without filters *)
  let joined = join_orders_and_items orders order_items () in
  assert_equal ~msg:"Should join and find 5 order items matching the 4 orders" 5 (List.length joined);
  
  (* Verify the item for non-existent order 5 was excluded *)
  assert_bool "Order item for non-existent order 5 should be excluded" 
    (not (List.exists (fun item -> item.order_id = 5) joined));
  
  (* Join with status filter *)
  let pending_joined = join_orders_and_items orders order_items ~status_filter:"Pending" () in
  assert_equal ~msg:"Should find 2 order items for pending orders" 2 (List.length pending_joined);
  assert_bool "All items should be for order 1" 
    (List.for_all (fun item -> item.order_id = 1) pending_joined);
  
  (* Join with origin filter *)
  let p_origin_joined = join_orders_and_items orders order_items ~origin_filter:"P" () in
  assert_equal ~msg:"Should find 3 order items for P-origin orders" 3 (List.length p_origin_joined);
  assert_bool "Items should be for orders 1 and 3" 
    (List.for_all (fun item -> item.order_id = 1 || item.order_id = 3) p_origin_joined);
  
  (* Join with both filters *)
  let filtered_joined = join_orders_and_items orders order_items 
    ~status_filter:"Complete" ~origin_filter:"O" () in
  assert_equal ~msg:"Should find 2 order items for complete O-origin orders" 
    2 (List.length filtered_joined);
  assert_bool "Items should be for orders 2 and 4" 
    (List.for_all (fun item -> item.order_id = 2 || item.order_id = 4) filtered_joined)

(* Test get_year_month function *)
let test_get_year_month _ =
  assert_equal ~msg:"Should extract year and month correctly"
    (2024, 1) (get_year_month date1);
  assert_equal ~msg:"Should extract year and month correctly"
    (2024, 2) (get_year_month date2)

(* Test aggregate_by_month_year function *)
let test_aggregate_by_month_year _ =
  let monthly_data = aggregate_by_month_year orders order_items in
  
  (* Should aggregate by month and year, and calculate averages *)
  assert_equal ~msg:"Should have 3 month-year groups" 3 (List.length monthly_data);
  
  (* Verify correct sorting by date *)
  let ((first_year, first_month), _, _) = List.nth monthly_data 0 in
  let ((second_year, second_month), _, _) = List.nth monthly_data 1 in
  let ((third_year, third_month), _, _) = List.nth monthly_data 2 in
  
  assert_equal ~msg:"First month should be January 2024" (2024, 1) (first_year, first_month);
  assert_equal ~msg:"Second month should be February 2024" (2024, 2) (second_year, second_month);
  assert_equal ~msg:"Third month should be March 2024" (2024, 3) (third_year, third_month);
  
  (* Check the January 2024 averages (2 items from order 1) *)
  let (_, jan_avg_amount, jan_avg_tax) = List.nth monthly_data 0 in
  assert_equal ~msg:"January average amount should be 20.0" ~cmp:(fun a b -> abs_float (a -. b) < 0.01)
    20.0 jan_avg_amount;
  assert_equal ~msg:"January average tax should be 1.5" ~cmp:(fun a b -> abs_float (a -. b) < 0.01)
    1.5 jan_avg_tax

(** Test suite *)
let suite = 
  "pure_functions_test_suite" >::: [
    "test_aggregate_order_items" >:: test_aggregate_order_items;
    "test_filter_orders_status" >:: test_filter_orders_status;
    "test_filter_orders_origin" >:: test_filter_orders_origin;
    "test_filter_orders_combined" >:: test_filter_orders_combined;
    "test_join_orders_and_items" >:: test_join_orders_and_items;
    "test_get_year_month" >:: test_get_year_month;
    "test_aggregate_by_month_year" >:: test_aggregate_by_month_year;
  ]

(** Run the tests *)
let () = 
  run_test_tt_main suite