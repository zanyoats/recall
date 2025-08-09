sales(space_odyssey, philly, 12, 100).
sales(space_odyssey, philly, 18, 3).
sales(space_odyssey, nyc, 14, 77).
sales(space_odyssey, la, 13, 799).

sales(groundhog_day, philly, 8, 127).
sales(groundhog_day, nyc, 9, 61).
sales(groundhog_day, la, 11, 44).

sales(total_recall, la, 7, 404).

product_info_all_cities(Product, sum<Sales>, min<Cost>, max<Cost>) :-
  sales(Product, Ignore, Cost, Sales).

product_info_by_city(Product, City, sum<Sales>, min<Cost>, max<Cost>) :-
  sales(Product, City, Cost, Sales).

num_products(count<Product>) :-
  product_info_all_cities(Product, Ignore1, Ingore2, Ignore3).

total_sales(sum<Sales_By_Product>) :-
  product_info_all_cities(Ignore1, Sales_By_Product, Ingore2, Ignore3).
