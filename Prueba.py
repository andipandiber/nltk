import csv,re,string
import nltk
from stop_words import get_stop_words
import requests
import psycopg2

# Funcion para eliminar los puntos y Comas
def remove ( text ):
    return re.sub('[%s]' % re.escape(string.punctuation), ' ', text)


# Funcion para sacar los links
def links(url):
    urls = []
    with requests.get(url) as pagina:
        html = str(pagina.content,'utf-8')
        patron = r"<div\sclass=\"wXUyZd\"><a href=\"(.*?)\""
        linksNav = re.finditer(patron, html, re.MULTILINE)
        for contador, encontro in enumerate(linksNav, start=1):
            urls.append("https://play.google.com"+re.sub(r"<[^>]*>", "", encontro.group(1)))
    return urls

# Funcion para sacar los Titulos de las Paginas
def Titulos(url):
    with requests.get(url) as pagina:
        html = str(pagina.content,'utf-8')
        patron2 = r"<h1 class=\"AHFaub\" itemprop=\"name\"><span >(.*)</span></h1>"
        titulo = re.finditer(patron2, html, re.MULTILINE)
        for contador, encontro in enumerate(titulo, start=1):
            return (re.sub(r"<[^>]*>", "", encontro.group(1)))

# Funcion para sacar la Descripcion de las Paginas
def Descripcion(url):
    with requests.get(url) as pagina:
        html = str(pagina.content,'utf-8')
        patron3 = r"<div jsname=\"sngebd\">(.*?)</div>"
        descr = re.finditer(patron3, html, re.MULTILINE)
        for contador, encontro in enumerate(descr, start=1):
            return (re.sub(r"<[^>]*>", "", encontro.group(1)))

def Ranking(url):
    with requests.get(url) as pagina:
        html = str(pagina.content,'utf-8')
        patron3 = r"div class=\"BHMmbe\" aria-label=\"(.*?)\">(.*?)</div>"
        descr = re.finditer(patron3, html, re.MULTILINE)
        for contador, encontro in enumerate(descr, start=1):
            return (re.sub(r"<[^>]*>", "", encontro.group(2)))

def Comentarios(url):
    comentar = []
    with requests.get(url) as pagina:
        html = str(pagina.content,'utf-8')
        patron3 = r",[4,5],null,\"(.*?),\["
        comenta = re.finditer(patron3, html, re.MULTILINE)
        for contador, encontro in enumerate(comenta, start=1):
            comentar.append(re.sub(r"<[^>]*>", "", encontro.group(1)))
    return comentar


# Bucle para Busqueda dentro del CSV
with open ('/Users/andres/Documents/Base.csv', encoding="utf-8") as lineas:
    reader = csv.reader(lineas,delimiter=",")
    contador=0
    # Bucle para recorrer el CSV
    for row in reader:
       linea=row
       texto=remove(str(linea))
       # Realizar la Tokenizacion de cada Linea
       division=nltk.word_tokenize(str(texto))
       SPANISH_STOPWORDS = get_stop_words('spanish')
       # Busqueda dentro del Alfabeto de StopWords
       division_filtrado=[palabra for palabra in division if(palabra not in SPANISH_STOPWORDS)]
       STEM_SPANISH = nltk.stem.SnowballStemmer('spanish')
       # STEM de la division_filtrado
       stem=[]
       for palabra in division_filtrado:
           stem.append("{0}".format(STEM_SPANISH.stem(palabra)))
        # Crear la Url de Busqueda
       busca='%2B'.join(map(str,stem))
       url="https://play.google.com/store/search?q="+str(busca)+"&c=apps&hl=es"
       link=links(url)
       etiquetado=nltk.pos_tag(stem)
       cajaPalabra=set()
       for palabra in etiquetado:
           cajaPalabra.add(palabra)
       #print(cajaPalabra)
       for item in link:
           titulo=Titulos(item)
           descripcion=Descripcion(item)
           rank=Ranking(item)
           comenta=Comentarios(item)
           for ite in comenta:
               contador = contador + 1
               print(item)
               conexion = psycopg2.connect(host="localhost", database="prueba", user="postgres", password="pandi123")
               cur = conexion.cursor()
               sqlquery = "insert into prueba(codigo,palabra, url, titulo, urls, descripcion, ranking,comentarios) values (%s,%s,%s,%s,%s,%s,%s,%s)"
               datos = (contador, busca, url, titulo, item, descripcion, rank, ite)
               print(datos)
               cur.execute(sqlquery, datos)
               conexion.commit()



