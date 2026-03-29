from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from database import Base
import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Sao_Paulo")

class Veiculo(Base):
    __tablename__ = "veiculos"
    chassi = Column(String, primary_key=True, index=True)
    modelo = Column(String)
    ordem = Column(Integer)
    ar_condicionado = Column(String)
    cj_bco = Column(String)
    cliente = Column(String)
    destino = Column(String)
    localizacao = Column(String)
    banco_presente = Column(String)
    banco_comentario = Column(String)

class Apontamento(Base):
    __tablename__ = "apontamentos"
    id = Column(Integer, primary_key=True, index=True)
    chassi = Column(String, ForeignKey("veiculos.chassi"))
    etapa = Column(String)
    status = Column(String)
    responsavel = Column(String)
    inicio = Column(DateTime(timezone=True))
    termino = Column(DateTime(timezone=True))
    localizacao = Column(String)
    observacao = Column(String)

class Historico(Base):
    __tablename__ = "historico"
    id = Column(Integer, primary_key=True, index=True)
    chassi = Column(String)
    modelo = Column(String)
    etapa = Column(String)
    status = Column(String)
    responsavel = Column(String)
    inicio = Column(DateTime(timezone=True))
    termino = Column(DateTime(timezone=True))
    localizacao = Column(String)
    observacao = Column(String)
    data_apontamento = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(LOCAL_TZ))


class Usuario(Base):
    __tablename__ = "usuarios"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String, nullable=False)
    login = Column(String, unique=True, index=True, nullable=False)
    senha_hash = Column(String, nullable=False)
    perfil = Column(String, nullable=False)
    criado_em = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(LOCAL_TZ))


class PostoSequencia(Base):
    __tablename__ = "posto_sequencias"
    id = Column(Integer, primary_key=True, index=True)
    posto = Column(String, index=True, nullable=False)
    chassi = Column(String, ForeignKey("veiculos.chassi"), nullable=False)
    sequencia = Column(Integer, nullable=False)
    criado_em = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(LOCAL_TZ))


class OrdemServico(Base):
    __tablename__ = "ordens_servico"
    id = Column(Integer, primary_key=True, index=True)
    chassi = Column(String, ForeignKey("veiculos.chassi"), unique=True, index=True, nullable=False)
    nome_arquivo = Column(String, nullable=False)
    caminho_arquivo = Column(String, nullable=False)
    criado_em = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(LOCAL_TZ))


class BomItem(Base):
    __tablename__ = "bom_itens"
    id = Column(Integer, primary_key=True, index=True)
    tipo = Column(String, index=True, nullable=False)
    chassi = Column(String, ForeignKey("veiculos.chassi"), index=True, nullable=False)
    cod_item = Column(String)
    item = Column(String)
    descricao = Column(String)
    qtd = Column(String)
    status = Column(String, default="NAO")
    responsavel = Column(String)
    atualizado_em = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(LOCAL_TZ))


class Empenho(Base):
    __tablename__ = "empenhos"
    id = Column(Integer, primary_key=True, index=True)
    bom_item_id = Column(Integer, ForeignKey("bom_itens.id"), nullable=False)
    chassi = Column(String, ForeignKey("veiculos.chassi"), index=True, nullable=False)
    cod_item = Column(String)
    item = Column(String)
    descricao = Column(String)
    qtd_empenhada = Column(Float, nullable=False)
    sequencia_producao = Column(Integer)
    responsavel = Column(String)
    criado_em = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(LOCAL_TZ))
