# Política de segurança

## Como reportar uma vulnerabilidade

Use o **reporte privado de vulnerabilidades** do GitHub: aba *Security* → *Report a vulnerability*. O relato fica visível só para o mantenedor até a correção. Não abra issue pública para falhas de segurança.

Descreva o problema, como reproduzir e o impacto esperado.

## Escopo

Vale para o código deste repositório: `src/` (extract, transform, load e leitor DBF), os testes e o workflow do CI. Exemplos do que interessa:

- leitura de um `.dbc`/`.dbf` malformado que cause execução de código, consumo descontrolado de memória ou dados gravados errados sem erro;
- dependência com vulnerabilidade conhecida (o CI roda `pip-audit` e o Dependabot acompanha as versões);
- workflow do GitHub Actions com permissão além de `contents: read` ou que exponha segredos.

Fora do escopo: a infraestrutura do DATASUS (FTP sem TLS, ausência de hash publicado), o conteúdo dos microdados publicados pelo Ministério da Saúde e o Power BI. A limitação do FTP está descrita em *Limitações conhecidas* no README.

## Dados e LGPD

**Fonte.** O pipeline usa só os microdados públicos do SIH/SUS (AIH Reduzida, arquivos `RD`), publicados pelo Ministério da Saúde no FTP do DATASUS. Os arquivos não trazem nome, CPF nem endereço do paciente. Trazem, como publicados, o CEP de residência e a data de nascimento, além do CPF do gestor que autorizou a AIH (campo `GESTOR_CPF`).

**O que fica fora do git.** Nenhum dado, bruto ou processado, é versionado: a pasta `data/`, os logs e os arquivos `*.pbix` (que carregam os dados importados) estão no `.gitignore`. Os testes usam só AIHs sintéticas.

**O que o pipeline grava em disco**, sempre dentro do projeto:

| Caminho | Conteúdo | Campos sensíveis |
|---|---|---|
| `data/raw/RD{UF}{AAMM}.dbc` | arquivo original do DATASUS, SP inteiro | todos os publicados, inclusive CEP, nascimento e `GESTOR_CPF` |
| `data/converted/RD{UF}{AAMM}.parquet` | mesmo conteúdo do `.dbc`, todas as linhas e colunas | idem |
| `data/trusted/trusted_internacoes.parquet` | só hospitais da Baixada Santista, 17 colunas | nenhum: sem CEP, sem data de nascimento (só idade em anos), sem CPF |
| `data/database/hospital_data.db` | Star Schema no SQLite, a partir da camada trusted | nenhum, pelos mesmos motivos |
| `logs/pipeline.log` | etapas, contagens e erros | nenhum valor de registro, só totais |

Os arquivos intermediários (`.part` do download, da conversão e do transform, e o `.dbf` descompactado) são apagados ao final de cada etapa, inclusive em caso de erro.
