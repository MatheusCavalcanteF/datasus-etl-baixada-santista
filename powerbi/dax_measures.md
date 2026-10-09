# Medidas DAX — Painel de Internações SUS da Baixada Santista

Fonte: `data/database/hospital_data.db`, o Star Schema gerado por `python -m src.load`.

Os valores de referência foram calculados direto no SQLite, com as competências de jan/2025 a jul/2026. Use-os (seção 3) para conferir se o Power BI está batendo.

---

## 1. Preparar o modelo antes das medidas

As medidas de tempo dependem destes passos. Sem eles, o `DATEADD` falha ou devolve valores errados.

1. **Conexão.** Instale o driver ODBC do SQLite de 64 bits (instalador `sqliteodbc_w64`, de Christian Werner). Depois use *Obter dados → ODBC* com esta string, trocando o caminho pelo da sua pasta do projeto e ajustando o nome do driver se o Administrador de Fontes de Dados ODBC mostrar outro:
   ```
   Driver={SQLite3 ODBC Driver};Database=C:\caminho\para\Dados-Hospitalares-SUS\data\database\hospital_data.db
   ```
   Importe as 5 tabelas.
2. **Tipos no Power Query.** O SQLite não tem tipo data, então confira os tipos mesmo que o driver já reconheça as datas:
   - `Dim_Tempo[data]`, `Fato_Internacao[data_saida]`, `[data_internacao]` e `[competencia]` → **Data**
   - `Fato_Internacao[obito]` e `[dias_permanencia]` → **Número inteiro**
   - `Fato_Internacao[valor_total]` → **Número decimal fixo**
   - Códigos (`id_hospital`, `id_cid`, `id_municipio_residencia`, `id_municipio`) → **Texto**, porque o CNES tem zeros à esquerda
3. **Relacionamentos**, todos muitos-para-um com filtro em direção única:

   | De `Fato_Internacao` | Para | Status |
   |---|---|---|
   | `id_hospital` | `Dim_Hospital[id_hospital]` | Ativo |
   | `id_municipio_residencia` | `Dim_Municipio[id_municipio]` | Ativo |
   | `id_cid` | `Dim_CID[id_cid]` | Ativo |
   | `data_saida` | `Dim_Tempo[data]` | **Ativo** (eixo principal) |
   | `data_internacao` | `Dim_Tempo[data]` | Inativo |
   | `competencia` | `Dim_Tempo[data]` | Inativo |

4. **Marque `Dim_Tempo` como tabela de datas**, pela coluna `data`. É isso que faz o `DATEADD` funcionar quando o visual filtra por `ano_mes` ou `nome_mes`.
5. Desligue *Arquivo → Opções → Carregamento de dados → Data/hora automática*. Essa opção cria tabelas de datas ocultas que competem com a `Dim_Tempo`.
6. Em `Dim_Tempo[nome_mes]`, use *Classificar por coluna* → `mes`.
7. Oculte as colunas de chave do fato e crie as medidas numa tabela própria (por exemplo, *Inserir dados* → tabela vazia `_Medidas`).
8. **Separadores.** As fórmulas usam `,` entre argumentos. Se o seu Power BI estiver com os separadores regionais do pt-BR, ele vai exigir `;`. Troque nas fórmulas ou ajuste em *Arquivo → Opções → Configurações regionais*.

> **Não use `FORMAT()` para formatar medidas.** Ele devolve texto e quebra gráficos, ordenação e formatação condicional. Defina o formato em *Ferramentas de medida → Formato*; a string de formato de cada medida está indicada abaixo.

---

## 2. Medidas

Crie as medidas na ordem abaixo, porque algumas usam as anteriores.

### 2.1 Total de Internações

**O que mede:** quantas internações SUS terminaram (pela data de saída) nos hospitais da Baixada. É a base para dimensionar a demanda e o denominador das outras taxas.

**Por que filtrar por `tipo_aih`:** uma internação longa gera uma AIH por mês, todas com o mesmo número. Só a AIH `Normal` representa a internação; contar todas as linhas inflaria o volume. O TabNet usa o mesmo critério.

```dax
Total de Internações =
CALCULATE (
    COUNTROWS ( Fato_Internacao ),
    KEEPFILTERS ( Fato_Internacao[tipo_aih] = "Normal" )
)
```

O `KEEPFILTERS` respeita um filtro de `tipo_aih` que já exista no relatório, em vez de sobrescrevê-lo.

**Formato:** `#,0` · **Referência:** 144.812

### 2.2 Média de Permanência (ALOS)

**O que mede:** quantos dias, em média, um paciente ocupa o leito. É o principal indicador de giro de leitos. Uma ALOS alta significa menos pacientes atendidos por leito e pode indicar casos complexos, espera por exames ou altas atrasadas. Vale comparar por hospital, especialidade e CID, não só no total.

**Diferença em relação a uma média simples de `dias_permanencia`:** a média por linha conta cada AIH mensal de uma internação longa como uma internação separada. Neste período, as 167 internações longas dão 11,2 dias por linha contra 23,5 dias por internação. No total a diferença é pequena (5,37 contra 5,38 dias), mas em hospitais com muitos casos longos, como os de saúde mental, ela distorce. Por isso a medida divide a soma de todos os dias pelo número de internações.

```dax
Dias de Permanência =
SUM ( Fato_Internacao[dias_permanencia] )
```

```dax
Média de Permanência (ALOS) =
DIVIDE ( [Dias de Permanência], [Total de Internações] )
```

**Formato:** `0.0` · **Referência:** 5,4 dias (5,381)

### 2.3 Ticket Médio por AIH

**O que mede:** o valor médio que o SUS pagou por AIH, pela tabela SIGTAP. Serve de termômetro da complexidade dos atendimentos e da receita SUS por internação. **Não é o custo do hospital**, que costuma ficar bem acima da tabela.

A medida exclui as AIHs com valor zero. Hoje são 684, todas de leitos de saúde mental (especialidade 87), pagos fora da AIH. Incluí-las puxaria a média para baixo sem que o atendimento tenha sido barato.

```dax
Ticket Médio por AIH =
CALCULATE (
    AVERAGE ( Fato_Internacao[valor_total] ),
    KEEPFILTERS ( Fato_Internacao[valor_total] > 0 )
)
```

**Formato:** `"R$" #,0.00` · **Referência:** R$ 1.691,21

### 2.4 Taxa de Mortalidade

**O que mede:** a proporção de internações que terminaram em óbito. Interprete com cuidado: hospitais de referência recebem os casos mais graves, então uma taxa maior não significa, sozinha, pior qualidade. Compare hospitais dentro do mesmo CID ou especialidade.

O numerador conta os óbitos de todas as AIHs, inclusive as de longa permanência. O paciente morreu durante uma internação que já entra uma única vez no denominador.

```dax
Total de Óbitos =
SUM ( Fato_Internacao[obito] )
```

```dax
Taxa de Mortalidade =
DIVIDE ( [Total de Óbitos], [Total de Internações] )
```

**Formato:** `0.00%` · **Referência:** 5,44% (7.871 óbitos)

### 2.5 Variação MoM de Internações

**O que mede:** quanto o volume de internações cresceu ou caiu em relação ao mês anterior. Sinaliza mudanças de demanda, como sazonalidade respiratória, surtos ou fechamento de leitos.

**Proteção contra dado incompleto.** O SIH organiza os arquivos pelo mês de processamento, não pelo mês da alta. Medido nas altas de jan–jun/2025, que já têm mais de 12 meses de acompanhamento:

| Prazo após o mês da alta | Altas já processadas |
|---|---|
| no próprio mês | 87% |
| até 1 mês | 95% |
| até 2 meses | 98,7% |
| até 3 meses | 99,9% |

Sem proteção, a medida mostraria dois tipos de número falso:

- **Quedas nos últimos meses.** Julho/2026 apareceria com −7,2% só porque parte das altas ainda não foi processada.
- **Altas absurdas no começo da série.** Janeiro/2025 apareceria com +570%, porque dezembro/2024 só tem as AIHs que chegaram atrasadas.

Por isso a variação só aparece quando o mês está **Consolidado** e o mês anterior também tem dados completos. A medida `Status do Mês` explica no relatório por que um mês ficou em branco.

```dax
Internações Mês Anterior =
CALCULATE (
    [Total de Internações],
    DATEADD ( Dim_Tempo[data], -1, MONTH )
)
```

```dax
Primeira Competência Carregada =
CALCULATE ( MIN ( Fato_Internacao[competencia] ), REMOVEFILTERS () )
```

```dax
Última Competência Carregada =
CALCULATE ( MAX ( Fato_Internacao[competencia] ), REMOVEFILTERS () )
```

```dax
Status do Mês =
VAR _mesesParaConsolidar = 3 -- prazo para ~99,9% das altas chegarem ao SIH
VAR _inicioMes = MIN ( Dim_Tempo[data] )
RETURN
    IF (
        HASONEVALUE ( Dim_Tempo[ano_mes] ) && NOT ISEMPTY ( Fato_Internacao ),
        SWITCH (
            TRUE (),
            _inicioMes < [Primeira Competência Carregada], "Incompleto",
            EDATE ( _inicioMes, _mesesParaConsolidar ) > [Última Competência Carregada], "Preliminar",
            "Consolidado"
        )
    )
```

```dax
Variação MoM Internações % =
VAR _inicioMes = MIN ( Dim_Tempo[data] )
VAR _atual = [Total de Internações]
VAR _anterior = [Internações Mês Anterior]
VAR _comparavel =
    [Status do Mês] = "Consolidado"
        && EDATE ( _inicioMes, -1 ) >= [Primeira Competência Carregada]
RETURN
    IF ( _comparavel, DIVIDE ( _atual - _anterior, _anterior ) )
```

**Formato:** `+0.0%;-0.0%;0.0%`

**Uso:**
- Coloque `Dim_Tempo[ano_mes]` (ou ano + mês) no eixo. No total do ano a medida fica em branco de propósito.
- Para ver a variação sem a proteção, por exemplo numa análise exploratória, troque `_comparavel` por `TRUE ()`.
- Use `Status do Mês` na dica de ferramenta ou na formatação condicional, para avisar o leitor de que os meses preliminares ainda vão crescer.

**Cuidado na leitura:** fevereiro tem 28 dias e março tem 31. Só pela diferença de dias, março já teria +10,7%. Para comparar meses de tamanhos diferentes, internações por dia é uma medida mais justa.

---

## 3. Valores de referência

Calculados no SQLite com as competências de jan/2025 a jul/2026 (carga de 08/10/2026), pela data de saída. Para conferir, monte um visual de tabela com `Dim_Tempo[ano_mes]` e as medidas.

| Mês de saída | Status do Mês | Internações | ALOS | Ticket Médio | Mortalidade | Variação MoM |
|---|---|---:|---:|---:|---:|---:|
| 2024-09 | Incompleto | 2 | 31,0 | R$ 8.022,97 | 50,00% | — |
| 2024-10 | Incompleto | 67 | 6,3 | R$ 846,01 | 1,49% | — |
| 2024-11 | Incompleto | 368 | 4,7 | R$ 1.025,45 | 1,36% | — |
| 2024-12 | Incompleto | 1.119 | 5,8 | R$ 1.901,75 | 4,20% | — |
| 2025-01 | Consolidado | 7.498 | 5,3 | R$ 1.643,09 | 5,53% | — |
| 2025-02 | Consolidado | 7.114 | 5,3 | R$ 1.586,35 | 4,86% | −5,1% |
| 2025-03 | Consolidado | 7.839 | 5,4 | R$ 1.594,97 | 5,12% | +10,2% |
| 2025-04 | Consolidado | 7.814 | 5,3 | R$ 1.548,77 | 5,07% | −0,3% |
| 2025-05 | Consolidado | 8.058 | 5,3 | R$ 1.658,12 | 5,53% | +3,1% |
| 2025-06 | Consolidado | 7.438 | 5,5 | R$ 1.707,05 | 5,79% | −7,7% |
| 2025-07 | Consolidado | 7.791 | 5,3 | R$ 1.642,34 | 5,69% | +4,7% |
| 2025-08 | Consolidado | 7.604 | 5,3 | R$ 1.774,77 | 5,52% | −2,4% |
| 2025-09 | Consolidado | 7.772 | 5,4 | R$ 1.678,23 | 5,42% | +2,2% |
| 2025-10 | Consolidado | 7.973 | 5,3 | R$ 1.722,03 | 5,18% | +2,6% |
| 2025-11 | Consolidado | 7.339 | 5,4 | R$ 1.673,80 | 5,78% | −8,0% |
| 2025-12 | Consolidado | 7.395 | 5,6 | R$ 1.677,39 | 5,53% | +0,8% |
| 2026-01 | Consolidado | 7.314 | 5,6 | R$ 1.719,75 | 5,62% | −1,1% |
| 2026-02 | Consolidado | 7.114 | 5,5 | R$ 1.692,33 | 5,33% | −2,7% |
| 2026-03 | Consolidado | 8.041 | 5,1 | R$ 1.678,79 | 5,10% | +13,0% |
| 2026-04 | Consolidado | 7.863 | 5,2 | R$ 1.662,45 | 5,21% | −2,2% |
| 2026-05 | Preliminar | 7.285 | 5,3 | R$ 1.787,35 | 5,71% | — |
| 2026-06 | Preliminar | 7.265 | 5,4 | R$ 1.797,69 | 5,57% | — |
| 2026-07 | Preliminar | 6.739 | 5,6 | R$ 1.938,81 | 6,25% | — |
| **Total** | | **144.812** | **5,4** | **R$ 1.691,21** | **5,44%** | |

Sempre vai haver alguns meses "Incompleto" (hoje set–dez/2024) antes da primeira competência carregada. Eles só têm as altas antigas que chegaram atrasadas, e por isso o volume é pequeno e as taxas oscilam muito (set/2024: 2 internações, 50% de mortalidade).

Para escondê-los, filtre `Dim_Tempo[data]` a partir de 01/01/2025, o mês da primeira competência carregada.

- **Filtro de visual** (só nos gráficos mensais): esconde os meses e mantém os cartões de total em 144.812.
- **Filtro de página:** também tira dos totais essas 1.556 internações, que são reais. O total da página cai para 143.256.

### Se algo não bater

| Sintoma | Causa provável |
|---|---|
| Total de Internações = 145.004 | A medida está contando todas as AIHs, sem o filtro `tipo_aih = "Normal"` |
| Variação MoM em branco em todos os meses | `Dim_Tempo` não foi marcada como tabela de datas, ou o visual usa uma data do fato no eixo |
| Erro ao comparar valores do tipo Data com Texto | As colunas de data continuam como texto no Power Query |
| Números por mês diferentes da tabela acima | A relação ativa com a `Dim_Tempo` está em `data_internacao` ou `competencia`, e não em `data_saida` |
