class ExemploDataFrame():
    def __init__(self, spark, strArquivo):
        self.spark = spark
        self.Arquivo = strArquivo
        
    def ReturnDF(self):
        df = self.spark.read.csv(self.Arquivo)
        
        return df
    
    def ContarLinhas(self):
        df = self.ReturnDF()
        countDF = df.count()
        
        print("O dataframe possui " + str(countDF) + " linhas\n")
        
    def GravarParquet(self, name_file):
        df_write = self.ReturnDF()
        
        df_write.write.parquet(name_file, mode='append')
        
        print("Arquivo parquet criado com sucesso\n")
        
    def LerParquet(self, name_file):
        df_read = self.spark.read.parquet(name_file)
        df_read.show()