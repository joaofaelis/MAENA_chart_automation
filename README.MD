DOCUMENTATION CHART's MAENA
-
Código de automatização de geração de gráficos para relátorios de análises estátisticas.
 Foi desenvolvimento para uso próprio da empresa MAENA, para facilitar o processo de criação de relátorios para o cliente (CAMIL).

Arquitetura
-
Ele foi feito a partir da Onion Architecture adaptada, então é separado por:
- Infrastructure: Infraestrutura de conexão do Banco de dados, nesse caso SQL.
- Repository: Repositorio que possue as funções de consultas dentro do banco de dados, armazenando as querys necessárias.
- Services: Armazena as funções de serviços para criação dos graficos, cada grafico possui um serviço diferente.
- Main: Responsavel por Rodar o codigo chamando a Interface Grafica para consultas.

Funcionamento
-
O banco de dados utilizado possui procedures que utilizam de uma padronização de nome de Views. Nesse caso seria EX: "Escopo_1000",
cada View possui o nome "Escopo_" antes das numerações, essas views são responsáveis para realizar filtragem de dados a partir da Tabela Principal de dados, elas seguem uma hierarquia de filtragem especifica que serve para realizar calculos estatisticos quando rodado as Procedures.

Quando o "Main.py" é executado, a interface irá apresentar Inputs de Escopos (Cada escopo tendo um tema para geração de gráfico), basta colocar apenas o número que representa o Escopo e o periodo de data limite desejado para gerar os graficos.

(Por Optativa, uma pasta é criada no Desktop para armazernar o .XLSX com os graficos.)

Requirements
-
et-xmlfile==1.1.0
numpy==2.1.1
openpyxl==3.1.5
pandas==2.2.2
pyodbc==5.1.0
python-dateutil==2.9.0.post0
python-decouple==3.8
pytz==2024.1
six==1.16.0
tzdata==2024.1
XlsxWriter==3.2.0