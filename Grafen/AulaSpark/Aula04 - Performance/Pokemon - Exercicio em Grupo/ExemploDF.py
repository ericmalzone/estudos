class ExemploDataFrame():
    def __init__(self,spark, strArquivo, head=False):
        self.spark = spark
        self.Arquivo = strArquivo
        self.head = head
    
    def ReturnDF(self):
        df = self.spark.read.csv(self.Arquivo, header=self.head, sep=",")
        
        return df
    
    def ContarLinhas(self):
        df = self.ReturnDF()
        countDF = df.count()
        
        print("O dataframe possui " + str(countDF) + " linhas")
        
    def GravarParquet(self, name_file, df):
        #df_write = self.ReturnDF()
        
        df.write.parquet(name_file, mode='append')
        
        print("Arquivo parquet criado com sucesso")
        
    def LerParquet(self, name_file):
        df_read = self.spark.read.parquet(name_file)
        df_read.show()
    