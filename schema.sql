CREATE SCHEMA IF NOT EXISTS dw;

-- 1. Dimensão Tempo
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    sk_tempo INT PRIMARY KEY,
    data_completa DATE,
    ano INT,
    mes INT,
    nome_mes VARCHAR(20),
    trimestre INT,
    semana_ano INT,
    dia_semana VARCHAR(20)
);

-- 2. Dimensão Fornecedor
CREATE TABLE IF NOT EXISTS dw.dim_fornecedor (
    sk_fornecedor SERIAL PRIMARY KEY,
    id_fornecedor_original INT,
    nome_fornecedor VARCHAR(100),
    credit_rating INT,
    active_flag BOOLEAN,
    cidade VARCHAR(50),
    estado VARCHAR(50)
);

-- 3. Dimensão Produto
CREATE TABLE IF NOT EXISTS dw.dim_produto (
    sk_produto SERIAL PRIMARY KEY,
    id_produto_original INT,
    nome_produto VARCHAR(100),
    numero_produto VARCHAR(50),
    categoria VARCHAR(50),
    subcategoria VARCHAR(50),
    custo_padrao DECIMAL(18,2)
);

-- 4. Tabela Fato Compras
CREATE TABLE IF NOT EXISTS dw.fato_compras (
    id_fato SERIAL PRIMARY KEY,
    sk_produto INT REFERENCES dw.dim_produto(sk_produto),
    sk_fornecedor INT REFERENCES dw.dim_fornecedor(sk_fornecedor),
    sk_tempo_pedido INT REFERENCES dw.dim_tempo(sk_tempo),
    sk_tempo_vencimento INT REFERENCES dw.dim_tempo(sk_tempo),
    sk_tempo_envio INT REFERENCES dw.dim_tempo(sk_tempo),
    qtd_pedida INT,
    qtd_recebida DECIMAL(18,2),
    qtd_rejeitada DECIMAL(18,2),
    preco_unitario DECIMAL(18,2),
    total_linha DECIMAL(18,2),
    frete_unitario DECIMAL(18,2),
    id_pedido_compra INT
);