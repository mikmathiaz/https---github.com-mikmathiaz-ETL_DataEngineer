from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum # <-- A mudança chique está aqui
import pandas as pd
import sqlalchemy

# --- CONFIGURAÇÕES ---
CONN_DW = "dw_postgres"
CONN_OLTP = "oltp_sqlserver"

default_args = {
    'owner': 'Mikael',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# --- A DEFINIÇÃO DA DAG ---
with DAG(
    'etl_compras_adventureworks',
    default_args=default_args,
    description='ETL Completo de Compras: SQL Server -> Postgres',
    schedule=None, # "schedule_interval" mudou para "schedule" nas versoes novas
    start_date=pendulum.today('UTC').add(days=-1), # <-- Correção aqui
    catchup=False,
    tags=['compras', 'tcc'],
) as dag:

    # --- 1. CARGA DA DIMENSÃO TEMPO ---
    @task
    def carregar_dim_tempo():
        # Datas de 2010 a 2025
        datas = pd.date_range(start='2010-01-01', end='2025-12-31', freq='D')
        df = pd.DataFrame({'data_completa': datas})
        
        df['sk_tempo'] = df['data_completa'].dt.strftime('%Y%m%d').astype(int)
        df['ano'] = df['data_completa'].dt.year
        df['mes'] = df['data_completa'].dt.month
        df['nome_mes'] = df['data_completa'].dt.strftime('%B')
        df['trimestre'] = df['data_completa'].dt.quarter
        df['semana_ano'] = df['data_completa'].dt.isocalendar().week
        df['dia_semana'] = df['data_completa'].dt.strftime('%A')
        
        pg_hook = PostgresHook(postgres_conn_id=CONN_DW)
        engine = pg_hook.get_sqlalchemy_engine()
        
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("TRUNCATE TABLE dw.dim_tempo CASCADE;"))
            
        df.to_sql('dim_tempo', engine, schema='dw', if_exists='append', index=False)
        print(f"Dimensão Tempo carregada: {len(df)} linhas.")

    # --- 2. CARGA DA DIMENSÃO PRODUTO ---
    @task
    def carregar_dim_produto():
        mssql_hook = MsSqlHook(mssql_conn_id=CONN_OLTP)
        pg_hook = PostgresHook(postgres_conn_id=CONN_DW)
        
        sql = """
            SELECT 
                p.ProductID as id_produto_original,
                p.Name as nome_produto,
                p.ProductNumber as numero_produto,
                ISNULL(pc.Name, 'N/A') as categoria,
                ISNULL(ps.Name, 'N/A') as subcategoria,
                p.StandardCost as custo_padrao
            FROM Production.Product p
            LEFT JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
            LEFT JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
        """
        df = mssql_hook.get_pandas_df(sql)
        
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("TRUNCATE TABLE dw.dim_produto CASCADE;"))
            
        df.to_sql('dim_produto', engine, schema='dw', if_exists='append', index=False)
        print(f"Dimensão Produto carregada: {len(df)} linhas.")

    # --- 3. CARGA DA DIMENSÃO FORNECEDOR ---
    @task
    def carregar_dim_fornecedor():
        mssql_hook = MsSqlHook(mssql_conn_id=CONN_OLTP)
        pg_hook = PostgresHook(postgres_conn_id=CONN_DW)
        
        sql = """
            SELECT 
                v.BusinessEntityID as id_fornecedor_original,
                v.Name as nome_fornecedor,
                v.CreditRating as credit_rating,
                v.ActiveFlag as active_flag,
                ISNULL(a.City, 'Unknown') as cidade,
                ISNULL(sp.Name, 'Unknown') as estado
            FROM Purchasing.Vendor v
            LEFT JOIN Person.BusinessEntityAddress bea ON v.BusinessEntityID = bea.BusinessEntityID
            LEFT JOIN Person.Address a ON bea.AddressID = a.AddressID
            LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        """
        df = mssql_hook.get_pandas_df(sql)
        df = df.drop_duplicates(subset=['id_fornecedor_original'])

        engine = pg_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("TRUNCATE TABLE dw.dim_fornecedor CASCADE;"))
            
        df.to_sql('dim_fornecedor', engine, schema='dw', if_exists='append', index=False)
        print(f"Dimensão Fornecedor carregada: {len(df)} linhas.")

    # --- 4. CARGA DA FATO COMPRAS ---
    @task
    def carregar_fato_compras():
        mssql_hook = MsSqlHook(mssql_conn_id=CONN_OLTP)
        pg_hook = PostgresHook(postgres_conn_id=CONN_DW)
        
        # Query Origem
        sql_compras = """
            SELECT 
                pod.ProductID as id_produto_original,
                poh.VendorID as id_fornecedor_original,
                poh.OrderDate,
                pod.DueDate,
                poh.ShipDate,
                pod.OrderQty as qtd_pedida,
                pod.ReceivedQty as qtd_recebida,
                pod.RejectedQty as qtd_rejeitada,
                pod.UnitPrice as preco_unitario,
                pod.LineTotal as total_linha,
                poh.Freight,
                poh.PurchaseOrderID as id_pedido_compra
            FROM Purchasing.PurchaseOrderDetail pod
            JOIN Purchasing.PurchaseOrderHeader poh ON pod.PurchaseOrderID = poh.PurchaseOrderID
        """
        df_fatos = mssql_hook.get_pandas_df(sql_compras)
        
        # Tratamento de Datas
        df_fatos['sk_tempo_pedido'] = pd.to_datetime(df_fatos['OrderDate']).dt.strftime('%Y%m%d').fillna(19000101).astype(int)
        df_fatos['sk_tempo_vencimento'] = pd.to_datetime(df_fatos['DueDate']).dt.strftime('%Y%m%d').fillna(19000101).astype(int)
        df_fatos['sk_tempo_envio'] = pd.to_datetime(df_fatos['ShipDate']).dt.strftime('%Y%m%d').fillna(19000101).astype(int)

        # Lookup (Buscar SKs)
        engine = pg_hook.get_sqlalchemy_engine()
        df_dim_prod = pd.read_sql("SELECT sk_produto, id_produto_original FROM dw.dim_produto", engine)
        df_dim_forn = pd.read_sql("SELECT sk_fornecedor, id_fornecedor_original FROM dw.dim_fornecedor", engine)
        
        # Merge
        df_final = df_fatos.merge(df_dim_prod, on='id_produto_original', how='left')
        df_final = df_final.merge(df_dim_forn, on='id_fornecedor_original', how='left')
        
        # Frete Unitário
        df_final['frete_unitario'] = df_final['Freight'] / df_final['qtd_pedida']
        
        # Seleção Final
        colunas_finais = [
            'sk_produto', 'sk_fornecedor', 'sk_tempo_pedido', 'sk_tempo_vencimento', 'sk_tempo_envio',
            'qtd_pedida', 'qtd_recebida', 'qtd_rejeitada', 'preco_unitario', 'total_linha', 'frete_unitario',
            'id_pedido_compra'
        ]
        # Se alguma chave ficou NaN (produto não achado), preenche com 0 ou dropa. Vamos manter simples.
        df_load = df_final[colunas_finais].fillna(0)
        
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("TRUNCATE TABLE dw.fato_compras;"))
            
        df_load.to_sql('fato_compras', engine, schema='dw', if_exists='append', index=False)
        print(f"Fato Compras carregada: {len(df_load)} linhas.")

    # Orquestração
    [carregar_dim_tempo(), carregar_dim_produto(), carregar_dim_fornecedor()] >> carregar_fato_compras()