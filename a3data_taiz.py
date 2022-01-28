import pandas as pd
import numpy as np
import math

class aviation_occurencies:
    def __init__( self ):
        self.home_path = 'G:/DADOS/Documents/CURSOS_Dev/A3Data'
        
    def data_collect(self):
        ocorrencia = pd.read_csv('http://sistema.cenipa.aer.mil.br/cenipa/media/opendata/ocorrencia.csv', sep=';')
        tipo_ocorrencia = pd.read_csv('http://sistema.cenipa.aer.mil.br/cenipa/media/opendata/ocorrencia_tipo.csv', sep=';')
        aeronave = pd.read_csv('http://sistema.cenipa.aer.mil.br/cenipa/media/opendata/aeronave.csv', sep=';')
        fator_contribuinte = pd.read_csv('http://sistema.cenipa.aer.mil.br/cenipa/media/opendata/fator_contribuinte.csv', sep=';')
        rec_seguranca = pd.read_csv('http://sistema.cenipa.aer.mil.br/cenipa/media/opendata/recomendacao.csv', sep=';')


        #merge
        aux1 = pd.merge( ocorrencia, tipo_ocorrencia, how='left', on='codigo_ocorrencia1' )
        aux2 = pd.merge( aux1, aeronave, how='left', on='codigo_ocorrencia2' )
        aux3 = pd.merge( aux2, fator_contribuinte, how='left', on='codigo_ocorrencia3' )
        df_raw = pd.merge( aux3, rec_seguranca, how='left', on='codigo_ocorrencia4' )
    
        return df_raw

    

    def data_clenning(self, df1):
        #ocorrencia_dia - mudar para datetime 
        df1.ocorrencia_dia = pd.to_datetime(df1['ocorrencia_dia'], format='%d/%m/%Y')
        df1.ocorrencia_dia = pd.to_datetime(df1['ocorrencia_dia'], format='%Y-%m-%d')


        # recomendacao_dia_encaminhamento - 0 se dia for data com má formato 
        df1['recomendacao_dia_encaminhamento'] = df1['recomendacao_dia_encaminhamento'].apply( lambda x: 0 if ((x == '0002-11-29') or( x =='0002-11-24')or (x == '0002-11-17') or (x == '0002-11-27') or (x == '0002-11-26')) else x )
        df1['recomendacao_dia_encaminhamento'] =   pd.to_datetime(df1['recomendacao_dia_encaminhamento'], format= '%Y-%m-%d')


        #recomendacao_dia_feedback
        #df1['recomendacao_dia_feedback'] = df1['recomendacao_dia_feedback'].apply(lambda x: 0 if ((x == '0000-00-00') or (x == '0002-11-29') or (x == '0002-11-21') or (x == '0002-11-23')) else x)
        #df1['recomendacao_dia_feedback'] =   pd.to_datetime(df1['recomendacao_dia_feedback'], format= '%Y-%m-%d')

        #recomendacao_dia_assinatura
        df1['recomendacao_dia_assinatura'] =   pd.to_datetime(df1['recomendacao_dia_assinatura'], format= '%Y-%m-%d')
        # convertendo os dados numéricos
        for c in df1.select_dtypes( include=['int64'] ):
            df1[c] = df1[c].astype('int32')

        #   aeronave_ano_fabricacao         
        df1['aeronave_ano_fabricacao'] = df1['aeronave_fatalidades_total'].astype('int32')
        df1['aeronave_assentos'].fillna(0, inplace=True )
        df1.aeronave_assentos =df1['aeronave_assentos'].astype('int32')

        # convertendo os dados categóricos
        for c in df1.select_dtypes( include=['object'] ):
            df1[c] = df1[c].astype('category')

        #excluindo 
        df1.drop(['codigo_ocorrencia', 'codigo_ocorrencia1', 'codigo_ocorrencia2','codigo_ocorrencia3', 'codigo_ocorrencia4','divulgacao_relatorio_numero','ocorrencia_pais', 'recomendacao_numero', 'recomendacao_dia_feedback' , 'recomendacao_destinatario_sigla', 'aeronave_matricula', 'aeronave_pmd', 'aeronave_pmd_categoria'], axis=1, inplace=True)


        #correncia_latitude  como todas com na tem 'ocorrencia_cidade', colocar 0 para manter as linhas
        df1.dropna(subset =['ocorrencia_latitude'], inplace=True)


        #ocorrencia_longitude                1581
        df1.dropna(subset =['ocorrencia_longitude'], inplace=True)


        #ocorrencia_hora       drop
        df1.dropna(subset =['ocorrencia_hora'], inplace=True)

        #investigacao_aeronave_liberada - Suposições:
        ## SIM se investigação status = FINALIZADO e recomendacao_status == 'CUMPRIDA' ou "CUMPRIDA DE FORMA ALTERNATIVA "
        # NAO se divulgacao_relatorio_publicado == NAO 
        # os na´s que sobreram drop =647 
        df1.loc[ (df1['investigacao_aeronave_liberada'].isna()) & (df1['investigacao_status']== 'FINALIZADA') & (df1['recomendacao_status'] == ('CUMPRIDA' or 'CUMPRIDA DE FORMA ALTERNATIVA') ) , 'investigacao_aeronave_liberada'] = "SIM"
        df1.loc[ ( df1['investigacao_aeronave_liberada'].isna() ) & ( df1.divulgacao_relatorio_publicado == 'NÃO' ) , 'investigacao_aeronave_liberada'] = 'NÃO'
        df1.dropna(subset =['investigacao_aeronave_liberada'], inplace=True)


        #investigacao_status                    
        df1.dropna(subset =['investigacao_status'], inplace=True)


        #divulgacao_dia_publicacao           
        df1['divulgacao_dia_publicacao'] = df1['divulgacao_dia_publicacao'].cat.add_categories("0").fillna("0")

        #aeronave_voo_origem                    1
        df1.dropna(subset =['aeronave_voo_origem'], inplace=True)

        # aeronave_motor_tipo
        df1.dropna(subset =['aeronave_motor_tipo'], inplace=True)


        ## 4799  ou 28% do db como o obj. criei cat 'não informado'                         
        df1['fator_nome'] = df1['fator_nome'].cat.add_categories("não informado").fillna("não informado")                     
        df1['fator_aspecto'] = df1['fator_aspecto'].cat.add_categories("não informado").fillna("não informado")
        df1['fator_condicionante'] = df1['fator_condicionante'].cat.add_categories("não informado").fillna("não informado")
        df1['fator_area'] = df1['fator_area'].cat.add_categories("não informado").fillna("não informado")

        # recomendacao_dia_assinatura = quando publivcado = recomendacao_dia_assinatura == 'divulgacao_dia_publicacao'
        df1.loc[ df1['recomendacao_dia_assinatura'].isna(), 'recomendacao_dia_assinatura' ] = df1.loc[ df1['recomendacao_dia_assinatura'].isna(), 'divulgacao_dia_publicacao' ]

        # para ter esses valores, suponho que a divulgacao_relatorio_publicado == SIM investigacao_status == finalizado
        #recomendacao_dia_encaminhamento    5698
        # não foi publicado, não tem como ter folow up = divulgacao_relatorio_publicado == "NÃO", então 0.
        df1.loc[df1['recomendacao_dia_encaminhamento'].isna()  & (df1['divulgacao_relatorio_publicado']== 'NÃO') , 'recomendacao_dia_encaminhamento'] = 0
        df1['recomendacao_dia_encaminhamento'].fillna(0, inplace=True )


        # set categoria
        df1['recomendacao_conteudo'] = df1['recomendacao_conteudo'].cat.add_categories("NA - relatório Não publicado")

        # recomendacao_conteudo
        df1.loc[df1['recomendacao_conteudo'].isna()  & (df1['divulgacao_relatorio_publicado']== 'NÃO') , 'recomendacao_conteudo'] = "NA - relatório Não publicado"
        # o restante vou dropar 
        df1.dropna(subset =['recomendacao_conteudo'], inplace=True)

        # recomendacao_conteudo
        df1.loc[df1['recomendacao_status'].isna()  & (df1['divulgacao_relatorio_publicado']== 'NÃO') , 'recomendacao_status'] = "***"
        df1.dropna(subset =['recomendacao_status'], inplace=True)


        #set categoria
        df1['recomendacao_destinatario'] = df1['recomendacao_destinatario'].cat.add_categories("NA - relatório Não publicado")
        # recomendacao_destinatario
        df1.loc[df1['recomendacao_destinatario'].isna()  & (df1['divulgacao_relatorio_publicado']== 'NÃO') , 'recomendacao_destinatario'] = "NA - relatório Não publicado"

        return df1


    def filtering_data(self, df1):
        
        # Filtrando
        df2 = df1.loc[(df1['ocorrencia_latitude'] != "***") & (df1['ocorrencia_longitude'] != "***") & (df1.aeronave_motor_tipo != "***") & (df1.aeronave_nivel_dano != "***"), :]

        return df2


# Instancia
aviacao = aviation_occurencies()

#Coleta
data_raw = aviacao.data_collect()

#Limpa
df1 = aviacao.data_clenning(data_raw)

# Filtra
df2 = aviacao.filtering_data(df1)

print(df2.sample(3))