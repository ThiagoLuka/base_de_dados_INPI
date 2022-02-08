# Bibliotecas usadas:
import pandas as pd


# 2


# carregando os dados de depositantes
depositantes = pd.read_csv(
    'dados/PTN_DEPOSITANTES.csv',
    sep = ';',
    encoding = 'latin1',
    low_memory = False,
    usecols = ['NO_PEDIDO', 'NO_ORDEM', 'PAIS', 'UF', 'MUNICIPIO', 'CD_IBGE_CIDADE']
)


# selecionando os dados para o Brasil
selecao = depositantes['PAIS'] == 'BR'
depositantes = depositantes[selecao]
depositantes.drop('PAIS', axis='columns', inplace=True)

# selecionando o primeiro depositante
selecao = depositantes['NO_ORDEM'] == 1
depositantes = depositantes[selecao]
depositantes.drop('NO_ORDEM', axis='columns', inplace=True)


# 3


# removendo patentes sem o código de cidade
depositantes.dropna(subset=['CD_IBGE_CIDADE'], inplace=True)

# removendo patentes repetidas
depositantes.drop_duplicates(subset=['NO_PEDIDO'], inplace=True)

# removendo coluna dos municipios
depositantes.drop(columns=['MUNICIPIO'], inplace=True)


# 4


# carregando os dados de depositos
depositos = pd.read_csv(
    'dados/PTN_DEPOSITOS.csv',
    sep = ';',
    encoding = 'latin1',
    usecols = ['NO_PEDIDO', 'DT_DEPOSITO']
)

# convertendo datas para o formato datetime
depositos['DT_DEPOSITO'] = pd.to_datetime(depositos['DT_DEPOSITO'], errors='coerce')


# 5


# removendo depositos de patentes duplicados
depositos.drop_duplicates(subset=['NO_PEDIDO'], inplace=True)

# removendo datas de depósitos faltantes
depositos.dropna(subset=['DT_DEPOSITO'], inplace=True)


# 6


# carregando os dados de classificação das patentes
classificacoes = pd.read_csv(
    'dados/PTN_CLASSIFICACOES.csv',
    sep = ';',
    usecols = ['NO_PEDIDO', 'NO_ORDEM_PEDIDO', 'CD_CLASSIF']
)

# criando novos nomes
novos_nomes = {
    'NO_ORDEM_PEDIDO': 'IPC_ORDEM',
    'CD_CLASSIF': 'IPC_CD'
}

# renomeando
classificacoes.rename(columns=novos_nomes, inplace=True)


#  7

# carregando dados dos municípios
territorios = pd.read_excel(
    'dados/RELATORIO_DTB_BRASIL_MUNICIPIO.xls',
    usecols = [
        'Nome_UF',
        'Nome Região Geográfica Intermediária',
        'Nome Região Geográfica Imediata',
        'Nome_Mesorregião',
        'Nome_Microrregião',
        'Código Município Completo',
        'Nome_Município'
    ]
)

# criando novos nomes
novos_nomes = {
    'Nome Região Geográfica Intermediária': 'RG_INTERMED',
    'Nome Região Geográfica Imediata': 'RG_IMEDI',
    'Nome_Mesorregião': 'MESORREGIAO',
    'Nome_Microrregião': 'MICRORREGIAO',
    'Nome_Município': 'MUNICIPIO'
}

# renomeando
territorios.rename(columns=novos_nomes, inplace=True)


# 8


# unindo depositantes e territorios em dados
dados = pd.merge(
    depositantes, territorios,
    how='left',
    left_on='CD_IBGE_CIDADE',
    right_on='Código Município Completo',
    validate='many_to_one'
)

# removendo colunas relativas aos códigos de município
dados.drop(['Código Município Completo'], axis='columns', inplace=True)

# unindo dados e depositos
dados = pd.merge(
    dados, depositos,
    how='left',
    on='NO_PEDIDO',
    validate='one_to_one'
)

# unindo dados e classificacoes
dados = pd.merge(
    dados, classificacoes,
    how='left',
    on='NO_PEDIDO',
    validate='one_to_many'
)


# 9


# último tratamento dos dados, removendo dados faltantes
dados.dropna(subset=['IPC_CD', 'IPC_ORDEM'], inplace=True)

# otimização de memória
del depositantes
del territorios
del depositos
del classificacoes
dados['IPC_ORDEM'] = dados['IPC_ORDEM'].astype('int')

# reorganização dos dados
dados = dados.iloc[:, [0, 9, 11, 10, 1, 8, 4, 5, 6, 7, 3, 2]]


# 10


# função que gera a tabela

def gerar_tabela_RTA(df, uf='', agregar_por='UF', ipc=4, inicio=1996, fim=2020):
    # para evitar dupla contagem, utiliza-se somente o primeiro código ipc de cada patente
    df = df[df['IPC_ORDEM'] == 1]

    # selecionando os dados
    if uf != '':
        df = df[df['UF'] == uf]

    df = df[df['DT_DEPOSITO'].dt.year >= inicio]
    df = df[df['DT_DEPOSITO'].dt.year <= fim]

    # criando série de referência para distribuição proporcional geral
    grupo1 = df.groupby([df['IPC_CD'].str[0:ipc]]).size()
    serie_referencia = grupo1 / df.shape[0]

    # calculando a tabela com valores absolutos de pedidos de patentes
    grupo2 = df.groupby([agregar_por, df['IPC_CD'].str[0:ipc]], as_index=False).size()
    tabela_absolutos = grupo2.pivot(index='IPC_CD', columns=agregar_por, values='size')
    tabela_absolutos = tabela_absolutos.transform(lambda x: x.fillna(0))

    # tabela com valores absolutos contém a lista das regioes de acordo com a agregação...
    lista_regioes = list(tabela_absolutos.columns)

    # ... e serve de modelo para as tabelas com valores proporcionais e RTA!
    tabela_proporcional = tabela_absolutos
    tabela_RTA = tabela_absolutos

    # gerando tabela proporcional
    for regiao in lista_regioes:
        tabela_proporcional[regiao] = tabela_absolutos[regiao] / tabela_absolutos[regiao].sum()

    # gerando tabela RTA
    for regiao in lista_regioes:
        tabela_RTA[regiao] = (tabela_proporcional[regiao] >= serie_referencia).transform(lambda x: int(x))

    return tabela_RTA


# 11


# possíveis combinações:

# seleção por estado
ufs = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO',
       'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR',
       'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

# seleção por anos
anos = [i for i in range(1996, 2021)]  # há dados de 1996 até 2020

# agregação por nível regional
regioes = {
    1: 'UF',            # 'Nome_UF' também pode ser usada caso queira ver o nome completo das UFs
    2: 'RG_INTERMED',   # Região Geográfica Intermediária
    3: 'RG_IMEDI',      # Região Geográfica Imediata'
    4: 'MESORREGIAO',
    5: 'MICRORREGIAO',
    6: 'MUNICIPIO'
}

# agregação por número de dígitos do código IPC
ipc_digitos = [1, 2, 3, 4]  # recortes do IPC acima disso são muito específicos


# 12

# for regiao in regioes:
#     for ipc in ipc_digitos:
#         tabela_RTA = gerar_tabela_RTA(
#             dados,
#             uf='',
#             agregar_por=regiao,
#             ipc=ipc
#         )
#
#         nome = 'tabelas_excel/' + regiao + '-' + str(ipc) + 'digito_ipc' + '.xlsx'
#
#         print(nome, '\n', tabela_RTA)
#
#         tabela_RTA.to_excel(nome)


# tabela_RTA = gerar_tabela_RTA(
#     dados,
#     uf='MG',
#     agregar_por='MUNICIPIO',
#     ipc=4,
#     inicio=2000,
#     fim=2009
# )
#
# nome = 'tabelas_excel/' + 'MG-' + 'MUNICIPIO' + '-' + str(4) + 'digito_ipc-' + '2000-2009' + '.xlsx'
#
# print(nome, '\n', tabela_RTA)
#
# tabela_RTA.to_excel(nome)


print('Gerador de tabela RTA para dados do Brasil\n\n'
      'Aperte qualquer botão para continuar')

comando = input()

while comando != 'n':
    uf_bool = input('Restringir dados por alguma UFs? (s/n) ')
    if uf_bool == 's':
        print(f'Digite uma das ufs em maiúsculo. Por exemplo: MG')
        uf = input('UF: ')
    else:
        uf = ''
    print('\nEscolha algum nível de território para agregar os dados. Opções possíveis: \n')
    for key, regiao in regioes.items():
        print(f'Digite {key} para {regiao}')
    agregar_por = int(input('Agregar por: '))
    ipc = int(input('Agregar por quantos dígitos do IPC (1 a 4): '))
    tempo = input('Limitar dados por ano? (s/n) ')
    if tempo == 's':
        inicio = int(input('Início do período (1996 a 2020): '))
        fim = int(input('Fim do período (1996 a 2020): '))
        tempo_nome = str(inicio) + '-' + str(fim)
    else:
        inicio, fim = 1996, 2020
        tempo_nome = ''
    nome_arquivo = uf + '-' + regioes[agregar_por] + '-' + str(ipc) + 'digitos_ipc-' + tempo_nome + '.xlsx'

    tabela_RTA = gerar_tabela_RTA(
        dados,
        uf=uf,
        agregar_por=regioes[agregar_por],
        ipc=ipc,
        inicio=inicio,
        fim=fim
    )

    print(tabela_RTA)

    tabela_RTA.to_excel('tabelas_excel/' + nome_arquivo)

    print(f'Arquivo foi salvo na pasta tabelas_excel com o nome {nome_arquivo}')

    comando = input('Deseja continuar gerando tabelas? (s/n) ')

