# Copilot Workshop - Data Engineering Starter Code

Welcome to the Copilot Data Engineering Workshop! This repository provides you with a starter codebase to experiment with various data engineering tasks using Python.

## Prerequisites

Before you begin, make sure you have the following prerequisites installed on your system:

- Python 3 (Python 3.6 or higher is recommended)
- `pip` (Python package manager)
- **Java:** This repo pins **PySpark 3.3.4**, which works with **Java 8 or 11**. Set `JAVA_HOME` to that JDK.
  If you use a **newer unpinned PySpark (3.5+)**, you need **Java 17+**; otherwise you may see
  `UnsupportedClassVersionError` / class file version **61.0** vs **55.0**.


## Problem Statement

<b>The dataset used for this workshop is publically available [E-commerce Dataset](https://www.kaggle.com/datasets/mervemenekse/ecommerce-dataset) of an American company.</b>

In the context of an E-commerce business operating in America, they have collected a year's worth of transactional data from their customers. They aim to gain deep insights into their customers' online buying habits and identify opportunities for improving their services and revenue. To achieve this, they require your help to perform data transformation and analysis as part of their existing E-commerce ETL pipeline and dataset.

![Project Architecture](./docs/ecommerce_etl.drawio.png)


## Dataset columns and meaning

- `Order_Date`: The date the product was ordered.
- `Aging`: The time from the day the product is ordered to the day it is delivered.
- `Customer_id`: Unique ID created for each customer.
- `Gender`: Gender of the customer.
- `Device_Type`: The device the customer uses to complete the transaction (Web/Mobile).
- `Customer_Login_Type`: The type of customer login, such as Member, Guest, etc.
- `Product_Category`: Product category.
- `Product`: Product description.
- `Sales`: Total sales amount.
- `Quantity`: Unit amount of the product.
- `Discount`: Percent discount rate.
- `Profit`: Profit generated.
- `Shipping_cost`: Shipping cost.
- `Order_Priority`: Order priority, such as critical, high, etc.
- `Payment_method`: Payment method.


## Getting Started

Follow these steps to set up your environment and run the provided data engineering script:

1. Clone this repository to your local machine:

   ```bash
   git clone https://github.com/thoughtworks-hands-on/ecom-etl-data
   ```

2. Navigate to the project directory:

   ```bash
   cd ecom-etl-data
   ```

3. Create a virtual environment to isolate project dependencies:

   ```bash
   python3 -m venv env
   ```

4. Activate the virtual environment:

   ```bash
   source env/bin/activate
   ```

5. Install the required Python packages from the `requirements.txt` file:

   ```bash
   pip install -r requirements.txt
   ```


## Running the ETL Script

Now that you have set up your environment, you can run the ETL (Extract, Transform, Load) script to experiment with data engineering tasks.

Run the ETL from the **`ecom-etl-data`** folder using the **venv** where you installed `requirements.txt` (system `python3` often has no PySpark):

```bash
source env/bin/activate
pip install -r requirements.txt   # first time only
python etl.py
```

Or in one shot (no activate):

```bash
./env/bin/python etl.py
```

If you see `ModuleNotFoundError: No module named 'pyspark'`, you are not using that environment.

On **Python 3.12**, Spark’s `toPandas()` path can fail (`distutils` removed). This project loads the staging **Parquet** folder with **pandas + pyarrow** instead ([`src/jobs/load.py`](src/jobs/load.py)).

The script uses [`src/local_spark.py`](src/local_spark.py) so the driver binds to **`127.0.0.1`**, which avoids `BindException: Can't assign requested address` on some Mac/VPN/`/etc/hosts` setups.


## Working with `transform.py` and `src/transforms/`

Business transforms live in **`src/transforms/`** (one module per theme). **`src/jobs/transform.py`** chains them via `apply_all()` and writes Parquet.

| Module | Adds (examples) |
|--------|------------------|
| `time_based.py` | `shipping_speed_category` from `Aging` (express ≤2d, standard ≤6d, else delayed) |
| `customer_value.py` | `customer_total_sales`, `customer_total_profit`, `customer_value_segment` (tertiles) |
| `cumulative_discount.py` | `order_ts`, `cumulative_discount` (per customer, time-ordered) |
| `dynamic_shipping.py` | `dynamic_shipping_cost`, `profit_after_dynamic_shipping` |

Run tests (requires Java for PySpark):

```bash
pytest tests/jobs/test_transforms.py tests/jobs/test_extract.py
```

6. Make use of Copilot's code suggestions and autocompletion features to make your data transformation tasks more efficient and productive.


## Working with `analysis.ipynb`

Use the **same virtualenv as the ETL**. Dependencies like `matplotlib` and `scipy` are installed there.

### Cursor / VS Code

1. **Open the `ecom-etl-data` folder** as the workspace (or rely on [`.vscode/settings.json`](.vscode/settings.json), which points at `env/bin/python` when this folder is the workspace root).
2. Command Palette → **Python: Select Interpreter** → choose
   `.../ecom-etl-data/env/bin/python`
3. In the notebook, click the **kernel** name (top right) → **Select Another Kernel…** → pick that same interpreter.

### Register a named Jupyter kernel (optional)

```bash
cd ecom-etl-data
source env/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name=ecom-etl-data --display-name="Python (ecom-etl-data)"
```

Then in the notebook kernel picker, choose **Python (ecom-etl-data)**.

### Classic Jupyter

```bash
source env/bin/activate
jupyter notebook
```

3. Review the existing code to load the transformed data into the notebook.

4. Explore the data, perform statistical analysis, and create visualizations to gain insights from your data.

5. Feel free to experiment with copilot for different analysis techniques and visualizations to extract valuable information from your data.

6. Optionally, the tasks in analysis.ipynb can also be done by creating `analysis.py` file. Choose the option that best suits your workflow and preferences for data analysis.

Happy data engineering with Copilot! 🚀📊
