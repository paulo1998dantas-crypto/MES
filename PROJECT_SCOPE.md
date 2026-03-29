## Escopo de Trabalho

Este arquivo registra a logica acordada para a evolucao do projeto.
Ele sera a nossa referencia funcional daqui para frente.

## Direcao do Projeto

- Manter a logica atual do sistema como base.
- Continuar importando a base principal por planilha.
- Continuar resetando a base ativa ao importar uma nova base.
- Manter a exportacao para Excel como parte central do processo operacional.
- Evoluir o projeto por inclusao de funcionalidades, sem reescrever tudo.
- Priorizar um sistema simples, robusto e facil de operar.
- Considerar uso simultaneo de aproximadamente 15 pessoas.

## Perfis e Acessos

Os usuarios serao cadastrados pela interface do aplicativo.

Perfis iniciais:

- ADM
- LIDER
- VIDROS
- REVESTIMENTO
- DESMONTAGEM
- ELETRICA
- BANCO
- PREPARACAO
- EXPEDICAO
- SERRALHERIA
- LIBERACAO

## Regras Funcionais Acordadas

- A visao de ADM e LIDER deve preservar a logica geral atual do sistema.
- A visao do operador sera orientada por posto ou perfil operacional.
- O operador deve escolher o posto antes de atuar.
- O operador deve poder apenas iniciar, pausar e encerrar processo.
- O campo observacao sera opcional.
- Usuarios que nao sao ADM nem LIDER nao devem usar a visao geral atual.
- Usuarios PREPARACAO entram direto no posto PREPARACAO.
- Usuarios EXPEDICAO entram direto no posto EXPEDICAO.
- Usuarios LIBERACAO entram direto no posto LIBERACAO.
- Usuarios DESMONTAGEM, REVESTIMENTO, BANCO e SERRALHERIA usam a trilha operacional por posto.
- O usuario VIDROS deve enxergar o posto Corte de Vidro.
- O usuario SERRALHERIA deve enxergar os postos Tercerizacao e Serralheria Bancos.
- Etapas comuns de apontamento no fluxo atual ficam com ADM/LIDER: A/C, ACESSO. e PLOTA.
- VIDROS deve operar via posto Corte de Vidro.
- DESMONTAGEM deve operar via posto Desmontagem.
- ELETRICA deve operar via posto Eletrica, com sequenciamento proprio.
- REVESTIMENTO deve operar pelos postos Revestimento 1, 2, 3 e 4.
- BCO deve operar via posto Montagem Bancos.
- PREPARACAO deve operar em checklist B.O.M. proprio.
- EXPEDICAO deve operar em checklist B.O.M. proprio e com registro de empenho.
- LIBERACAO deve operar via posto Liberacao.
- O ADM define a fila de cada posto pela tela de sequenciamento.
- O ADM pode editar a sequência de cards já atribuídos e também excluir atribuições de posto pela tela de sequenciamento.
- O ADM deve ter um botao de sequenciamento automatico que recria as filas com base na logica atual do lobby principal e depois permite ajustes manuais.
- Para a etapa SERRA., o sequenciamento automatico deve atribuir apenas ao posto Tercerizacao.
- O operador enxerga cards sequenciados por posto e clica no card para iniciar, parar ou finalizar.
- O campo responsavel deve ser preenchido automaticamente pelo usuario logado nas acoes operacionais.
- Cada card pode ter uma O.S. em DOCX anexada.
- A O.S. deve ser visualizada no aplicativo, inclusive em mobile.
- PREPARACAO e EXPEDICAO recebem upload de B.O.M. por planilha separadamente.
- A B.O.M. de PREPARACAO e a B.O.M. de EXPEDICAO usam a mesma estrutura base: numero do chassi, codigo do item, item, descricao e quantidade.
- O bloco de upload da B.O.M. deve oferecer um botao para baixar o modelo padrao da planilha.
- EMPENHO deve ser gerado apenas na expedição e exportado separadamente pelo ADM.
- Na expedicao, o empenho deve registrar quantidade real consumida e permitir multiplos lancamentos para o mesmo item.
- A exportacao da B.O.M. de expedicao deve consolidar previsto x consumido por chassi e item, indicando saldo, faltante, conforme ou excedente.
- A exportacao de empenhos deve detalhar cada lancamento de consumo realizado na expedicao.
- Mesmo depois de a etapa EXPE. ja estar OK, novos lancamentos de empenho complementar podem acontecer e nao devem reabrir automaticamente a etapa para NAO.
- O perfil EXPEDICAO deve conseguir exportar o mesmo relatorio detalhado de lancamentos usado pelo ADM, com uma linha por saida de mercadoria.
- Na visao de LIDER, a etapa PREP nao pode ser marcada manualmente e so fica OK quando toda a B.O.M. da preparacao estiver OK pelo usuario operacional.
- Na visao de LIDER, a etapa EXPE. nao pode ser marcada manualmente e so fica OK quando toda a B.O.M. da expedicao estiver OK pelo usuario operacional.
- Na visao de ADM, a etapa PREP continua podendo ser marcada manualmente pela tela principal, se necessario.
- Na visao de ADM, a etapa EXPE. continua podendo ser marcada manualmente pela tela principal, se necessario.
- O apontamento manual do ADM em PREP ou EXPE. e temporario: se houver novo apontamento operacional no checklist, o checklist prevalece e sobrescreve o status manual.
- O ADM deve ter uma central de exportacoes com arquivos separados por modelo de dado.
- As telas principais do sistema devem atualizar automaticamente por polling leve para deixar o uso compartilhado o mais online possivel, sem atrapalhar quem estiver digitando.
- O sistema futuramente deve permitir upload de templates de O.S em DOCX.
- O DOCX devera ser visualizado dentro do aplicativo.
- O template em DOCX sera transformado em checklist estruturado de feito e nao feito.

## Etapas de Trabalho Ja Definidas

### Etapa 1

- Remover por enquanto a dependencia padrao do Supabase.
- Deixar o sistema funcional com banco local para testes.
- Manter a possibilidade de voltar ao Supabase depois.

### Etapa 2

- Criar login por usuario.
- Permitir cadastro de usuarios apenas pela interface do ADM.
- Aplicar os perfis padrao definidos neste documento.
