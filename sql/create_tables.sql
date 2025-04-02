CREATE TABLE IF NOT EXISTS atracacao_fato (
    id_atracacao INT PRIMARY KEY,
    cd_tup VARCHAR(10),
    id_berco VARCHAR(10),
    berco VARCHAR(100),
    porto_atracacao VARCHAR(100),
    apelido_instalacao VARCHAR(100),
    complexo_portuario VARCHAR(100),
    tipo_autoridade_portuaria VARCHAR(50),
    data_atracacao TIMESTAMP,
    data_chegada TIMESTAMP,
    data_desatracacao TIMESTAMP,
    ano INT,
    mes VARCHAR(10),
    tipo_operacao VARCHAR(50),
    tipo_navegacao VARCHAR(50),
    nacionalidade_armador VARCHAR(50),
    municipio VARCHAR(100),
    uf VARCHAR(50),
    sigla_uf VARCHAR(10),
    regiao_geografica VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS carga_fato (
    id_carga INT PRIMARY KEY,
    id_atracacao INT,
    origem VARCHAR(10),
    destino VARCHAR(10),
    cd_mercadoria VARCHAR(10),
    tipo_operacao VARCHAR(50),
    natureza_carga VARCHAR(50),
    sentido VARCHAR(20),
    teu INT,
    qt_carga INT,
    vl_peso_carga_bruta DOUBLE PRECISION,
    FOREIGN KEY (id_atracacao) REFERENCES atracacao_fato(id_atracacao)
);
