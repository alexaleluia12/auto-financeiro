
import json

from bs4 import BeautifulSoup

def main():

    file_name = "acoes.html"
    obj_saida = {
        "codigos": []
    }
    with open(file_name, encoding='latin1') as f:
        soap = BeautifulSoup(f.read(), 'html.parser')

    for link in soap.find_all('a'):
        if link.get('title') == "Comprar":
            parte = link.get('href')
            parte = parte.split('=')[-1]
            obj_saida["codigos"].append(parte)

    with open('codigos.json', mode='w') as f:
        json.dump(obj_saida, f)






if __name__ == '__main__':
    main()
