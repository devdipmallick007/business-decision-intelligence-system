from cleaning.customers_cleaning import main as run_customers
from cleaning.clean_products import main as run_products
from cleaning.clean_stores import main as run_stores
from cleaning.clean_sales import main as run_sales
from cleaning.clean_exchange_rates import main as run_exchange_rates  # fixed typo

def main():
    print("Starting full data cleaning pipeline...")

    # Run tables in proper order if needed
    run_customers()      # FK dependency for sales
    run_products()       # FK dependency for sales
    run_stores()         # Optional dependency for sales
    run_sales()          # Depends on customers, products, stores
    run_exchange_rates()    # Can run independently

    print("Pipeline finished successfully.")

if __name__ == "__main__":
    main()
