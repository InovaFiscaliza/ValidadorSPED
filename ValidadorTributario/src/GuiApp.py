#!/usr/bin/env python
# -*- coding: utf-8 -*-
import flet as ft
from flet import AppBar, ElevatedButton, Page, Text, View, colors,  Column, Container, Row
from markdownify import markdownify as md
from fletify import FletifyHTML
import uuid 
import base64
import subprocess
import webbrowser
from subprocess import check_output, Popen
import platform
import sys
import os
import io
from io import StringIO
import time
import shutil
from urllib.parse import urlparse, unquote, urlencode
import mimetypes
from pathlib import Path
import platform
from time import sleep
import random
import re
import chardet
from charset_normalizer import detect
import py7zr
import zipfile
import json
import paramiko
from nbformat import read, write, NO_CONVERT
from datetime import datetime
import calendar
import hashlib
import requests
import pandas as pd
import unicodedata
from cnpj import CNPJClient
import pyperclip
import pickle 
from pprint import pprint

### Autor Gui
###
### - Sérgio S Q 
###
###
### <!-- LICENSE -->
### # Licenças
### Distributed under the GNU General Public License (GPL), version 3. See [`LICENSE`](\LICENSE.md) for more information.
### For additional information, please check <https://www.gnu.org/licenses/quick-guide-gplv3.html>
### This license model was selected with the idea of enabling collaboration of anyone interested in projects listed within this group.
### It is in line with the **Brazilian Public Software directives**, as published at: <https://softwarepublico.gov.br/social/articles/0004/5936/Manual_do_Ofertante_Temporario_04.10.2016.pdf>
###

### O python instalou localmente a partir da loja, não foi necessário instalar versão "portable":
### pip install -r requirements.txt
### pip install PyInstaller
### Opção de interface nativa do SO ou WEB na linha final
### Gerando de forma simples um arquivo único executável distribuível:
### C:\Users\sergiosq\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.12_qbz5n2kfra8p0\LocalCache\local-packages\Python312\Scripts\flet pack main.py
### É necessário instalar o Flutter para gerar aplicativo instalável e publicar na Microsoft Store.
### https://flet.dev/ ; https://flutter.dev/ 


###
### Provisoriamente para testes iniciais a gui fica no memso arquivo
### "\nData da consulta na SRF: " + str(datetime.today()) + ""



guiapplink = "https://raw.githubusercontent.com/InovaFiscaliza/RepositorioFerramentasGRs/refs/heads/main/ValidadorTributario/dist/GuiApp" # "http://testando:13000/testes/ValidadorTributario/raw/master/GuiApp.py"
if platform.system() == "windows":
   guiapplink += ".exe"  

default_dockerfile = "https://raw.githubusercontent.com/InovaFiscaliza/RepositorioFerramentasGRs/refs/heads/main/ValidadorTributario/Dockerfile" # "http://testando:13000/testes/ValidadorTributario/raw/master/Dockerfile"

nomeapp = "Gui trivial para linha de comando"
nomeappgui = nomeapp

cmdline = "ping"

descapp = "Gui trivial para linha de comando"


appver = "Versão beta Gui 202412121850 " 

arquivosenha = "corrigir_este_codigo.txt"

txtwarn = "Gui trivial para linha de comando.\n"


maxbufff = 4096

cfg = {}
RepositorioAppLocal = str(Path(str(Path.home())).as_posix()) + "/RepositorioAppLocal" 
pastaRaiz = str(Path(str(Path.home())).as_posix())
log_ =  RepositorioAppLocal + "/tmp"
logcmd_ = '/tmp'

Path(log_).mkdir(parents=True, exist_ok=True)
if os.path.exists(f"{log_}/debug.log"):
  os.remove(f"{log_}/debug.log")
pathprestok = ""
host = ""
linscroll = 0
colscroll = 0
iduuid = ""
args = []

ddeep = 256
vpcnt = -1
tcodec = 0

link = ""
resultadosexistentes = False # corrigir erro
relanteriores = {}
test = "bWFpbHRvOnNlcmdpb3NxQGFuYXRlbC5nb3YuYnI/c3ViamVjdD1FcnJvcyUyMGUlMjBTdWdlc3QlQzMlQjVlcyUyMA" # eliminar variáveis globais, criar classes , etc ..


dtfatual = pd.DataFrame()


fmt = {}

def debug(msg):

    Path(log_).mkdir(parents=True, exist_ok=True)
    with open(f"{log_}/debug.log", 'a', encoding="utf-8") as file: # 
        file.write(f"\n{msg}")  

data0 = datetime.now()
print(f'variáveis iniciais {data0.strftime("%d/%m/%Y %H:%M:%S")}')

sshenv = ""
if "SSH_CLIENT" in os.environ:
    sshenv = os.environ['SSH_CLIENT']

debug("############################################################################################################")
debug(f"Executando {appver} {sshenv} {str(datetime.today())} ") # 
debug("############################################################################################################")
debug(os.environ)


def fmtnome(name):

    ret = ""
    name0 = ''.join(ch for ch in unicodedata.normalize('NFKD', name) 
        if not unicodedata.combining(ch))

    tmp = re.sub(r"[^A-Za-z0-9_ ]", " ", name0)
    tmp = tmp.title()
    ret = re.sub(r"[^A-Za-z0-9_]", "", tmp)

    return ret

        # https://www.digitaldesignjournal.com/subprocess-communicate/
def cmd_interface(cmd,comm): #  Popen(["python", cmdline, pastaRaiz , cnpj_ , ano_ ]) #  

    print(f"cmd_interface cmd: {cmd}")
    ret = {}
    if not comm:
        Popen(cmd)
    else:
        try:
            ret = json.loads(check_output(cmd))
        except:
            ret = check_output(cmd).decode("utf-8")

    return ret   

def salva_debug_backup(key, value):

    # voltar p/ sgared preferences
    sha256 = hashlib.sha256()
    sha256.update(nomeappgui.encode('utf-8'))

    pathori = f"{RepositorioAppLocal}/{sha256.hexdigest()}"

    tmp = os.path.split(pathori+"/"+key)
    Path(tmp[0]).mkdir(parents=True, exist_ok=True)

    os.path.split(pathori+"/"+key)

    with open(pathori+"/"+key, 'w', encoding="utf-8") as f:
        f.write(value)  
    return True

def le_offlinexxx(key):

    value = ""
    # if await page.client_storage.contains_key_async(str(key+nomeapp)):  # debug ?
    #     value = await page.client_storage.get_async(str(key+nomeapp))
    # else:
    #     pass #await salva_offline(str(key), default)  
    
    sha256 = hashlib.sha256()
    sha256.update(nomeappgui.encode('utf-8'))
    try:
        with open(RepositorioAppLocal+"/"+sha256.hexdigest()+"/"+key, 'r', encoding="utf-8") as file:
            value = file.read()
    except:
        pass 
        debug(f"erro le_offline: {RepositorioAppLocal}/{sha256.hexdigest()}/{key}")
    return value


def remove_offlinexxx(key):

    #page.client_storage # shutil.rmtree(RepositorioAppLocal+"/out") # debug ?
    sha256 = hashlib.sha256()
    sha256.update(nomeappgui.encode('utf-8'))
    if os.path.isfile(RepositorioAppLocal+"/"+sha256.hexdigest()+"/"+key):
        os.remove(RepositorioAppLocal+"/"+sha256.hexdigest()+"/"+key) 

    return True


def existexxx(key):

    value = False
    # if await page.client_storage.contains_key_async(str(key+nomeapp)):  # debug ?
    #     value = True
    sha256 = hashlib.sha256()
    sha256.update(nomeappgui.encode('utf-8'))
    value = os.path.isfile(RepositorioAppLocal+"/"+sha256.hexdigest()+"/"+key)

    return value

def lista_offlinexxx(filtro):

    res = []
    
    value = ""

    sha256 = hashlib.sha256()
    sha256.update(nomeappgui.encode('utf-8'))
    pathori = f"{RepositorioAppLocal}/{sha256.hexdigest()}"
    Path(pathori).mkdir(parents=True, exist_ok=True)

    paths = sorted(Path(pathori).iterdir(), key=os.path.getmtime)

    paths.reverse()
    for entry in paths:
        tmp = os.path.split(entry)
        #print(f"reverse paths entry: \n{tmp[1]} {st.st_mtime}\n")   
        partes = re.search(filtro, tmp[1] ) 
        if partes:
                st = entry.stat()
                print(f"config encontrado: {tmp[1]} {st.st_mtime}")
                res.append(tmp[1])
    
    print(f"lista_offline: \n{res}")
    return res      





# def recuperavpath(path):

#     global pastaRaiz, pasta_in, vpath


#     while path.find(pasta_in) != -1: # se ainda não chegou na pasta de origem
#         n1 = path[2+len(pasta_in):].find("/")
#         n = 2+len(pasta_in) + n1
#         path1 = path[:n]
#         path2 = path[n:]
#         debug(f"recuperavpath: >{path1}< >{path2}<")        
#         #tmp = os.path.dirname(path)
#         ori = vpath[vpath['destino'] == path1]
#         ori['origem'].iloc[0]
#         debug(ori)
#         debug(ori['origem'].iloc[0])
#         path = ori['origem'].iloc[0] + path2

#     debug(f"recuperavpath: >{path}<")
#     path = unquote(path)
#     return path


# def mostraraiz():
#     global pastaRaiz
#     try:
#         raiz = str(Path(pastaRaiz).as_uri())
#     except:
#         raiz = "file:///"    
#     return raiz
    

# def respostaokxxx(res0):

#     # ajuste de parêmetros para contagens e cores
#     res = res0.split('(', 1)[0] # desconsiderar comentários do proprio script
#     if re.search(r"não", res): ## resposta negativa
#         debug("-a")
#         n1 = 0
#         n2 = 1 
#     elif re.search(r"[a-zA-Z]", res) and not (re.search(r"não.*[^(]", res) or re.search(r"falha", res)): ## com resposta porém sem não da receita ou falha
#         debug("-b")
#         n1 = 1
#         n2 = 0
#     # elif re.search(r"falha", res): # sem resposta
#     #     debug("-c")
#     #     n1 = 0
#     #     n2 = 0
#     else:
#         debug("-c")
#         n1 = 0
#         n2 = 0

#     debug(f"respostaok(res) >{res}< {n1} {n1}")
#     return n1, n2   


# def meses_inclusos(ano , dataini, datafim, msg,m2): # !!!

#     global meses

#     debug(str(dataini))
#     debug(str(datafim))
#     debug(f"ano {ano}")




#     res = []
#     r2 = []
#     for mes in range(1, 13):
#         smes = str(mes)
#         if(mes < 10):
#             smes = "0" + str(mes)

#         if ano == 0:
#             res.append(0)
#             r2.append(0)
#             debug('meses inclusos ____')
#         else:    

#             try:
#                 n = calendar.monthrange(int(ano), int(smes))[1]
#                 ini = datetime.strptime(str(ano)+smes+"01",r'%Y%m%d')
#                 fim = datetime.strptime(str(ano)+smes+str(n),r'%Y%m%d')

#                 if datetime.strptime(dataini,r'%Y-%m-%d') <= fim and datetime.strptime(datafim,r'%Y-%m-%d') >= ini: # r'%Y-%m-%d %H:%M:%S'
#                     res.append(msg) ## res.append(meses[mes-1])
#                     r2.append(m2)
#                 else:
#                     res.append(0)
#                     r2.append(0)

#             except:
#                 res.append(0)
#                 r2.append(0)
#                 debug('meses inclusos ____')

#     debug(f"meses_inclusos {ano} {dataini} {datafim} {msg} {m2} ==> {res} {r2}")
#     return res, r2



async def main(page: ft.Page):



    async def salva_offline(key, value):

        value =  await page.client_storage.set_async(key, value)

        return value


    async def le_offline(key):

        value = await page.client_storage.get_async(key) 

        return value 


    async def remove_offline(key):

        value = await page.client_storage.remove_async(key)

        return value


    async def existe(key):

        value = await page.client_storage.contains_key_async(key)

        # value = await page.client_storage.get(key)

        # print(f"1 async def existe: {key} >{value}<") 

        # if value == "" | value == False :
        #     value = False
        # else:
        #     value = True  

        # print(f"2 async def existe: {key} >{value}<")  
        # 

        if value == None:
            value = False
        else:
            Value = True        

        return value 


    async def lista_offline(filtro):  

        res = await page.client_storage.get_keys_async(filtro)

        return res



    def atualizacoes():

        global cfg
        # esta gui
        try: 
            headers = {"Range": "bytes=0-0"}
            gui = requests.get(guiapplink, headers=headers)
            if gui.status_code == 200:
                try:
                    tst = gui.headers['Last-Modified']
                except:
                    tst = gui.headers['ETag']
                # comparar e tratar aqui versões antes de salvar em cfg['atualizacao-gui']
                if cfg['atualizacao-gui'] != tst:
                    cfg['versao-disponivel-gui'] = tst
                    print(f"*** cfg['versao-disponivel-gui']: {cfg['versao-disponivel-gui']}")
        except:
            pass


    async def config():

        global nomeapp, descapp, cmdline, appver, txtwarn, resultadosexistentes, relanteriores, cfg


        data0 = datetime.now()
        print(f"*** inicio config {data0}")
        print(f'*** inicio config {data0.strftime("%d/%m/%Y %H:%M:%S")}')

        if not await existe('cfg.json'):

            try:

                lista_offline(f'__config__(.*)__$')

                data0 = datetime.now()
                print(f'*** leu lista_offline {data0.strftime("%d/%m/%Y %H:%M:%S")}')

                cfg = {}
                cfg['versao-disponivel-gui'] = ""
                cfg['versao-disponivel-app'] = ""
                dockerfile = ""
                requirements = ""
                appfile  = ""

                # esta gui
                headers = {"Range": "bytes=0-0"}
                gui = requests.get(guiapplink, headers=headers)
                print(f"gui.headers: {gui.headers}")
                if gui.status_code == 200:
                    # comparar e tratar aqui versões antes de salvar em cfg['atualizacao-gui']
                    try:
                        cfg['atualizacao-gui'] = gui.headers['Last-Modified']
                    except:
                        cfg['atualizacao-gui'] = gui.headers['ETag']# ETag

                pathdockerfile = default_dockerfile
                path = pathdockerfile[:(pathdockerfile.rfind('/') + 1)] 
                dock = requests.get(pathdockerfile)
                headers = dock.headers
                # print(f"get dockerfile:\n{dock.headers['Last-Modified']}")
                # pprint(dock.headers)
                if dock.status_code == 200:
                    dockerfile = dock.text
                    for line in dockerfile.splitlines():
                        
                        partes = re.search(f'LABEL org.opencontainers.image.title (.*)', line )
                        if partes:
                            nomeapp0 = partes.group(1)
                        
                        partes = re.search(f'LABEL org.opencontainers.image.description (.*)', line )
                        if partes:
                            descapp0 = partes.group(1)
                        
                        partes = re.search('CMD . .python., ../(.*). .', line ) # f'CMD [ "python", "./(.*)" ]'
                        if partes:
                            cmdline0 = partes.group(1)

                if dockerfile != '':
                    req= requests.get(f"{path}requirements.txt")
                    if req.status_code == 200:
                        requirements = req.text

                if requirements != "":
                    req = requests.get(f"{path}{cmdline0}")
                    if req.status_code == 200:
                        appfile = req.text

                data0 = datetime.now()
                print(f'*** baixou atualização {data0.strftime("%d/%m/%Y %H:%M:%S")}')

                if appfile != "":

                    sha256 = hashlib.sha256()
                    sha256.update(pathdockerfile.encode('utf-8'))

                    salva_offline(f"GuiApp.py", gui.text)
                    salva_offline(f"{sha256.hexdigest()}/dockerfile", dockerfile)
                    salva_offline(f"{sha256.hexdigest()}/requirements.txt", requirements)
                    salva_offline(f"{sha256.hexdigest()}/{cmdline0}", appfile)
                    salva_offline(f"__config__{sha256.hexdigest()}__", json.dumps(cfg))

                    ### local após download ?

                    # debug e backup 
                    salva_debug_backup(f"GuiApp.py", gui.text)
                    salva_debug_backup(f"{sha256.hexdigest()}/dockerfile", dockerfile)
                    salva_debug_backup(f"{sha256.hexdigest()}/requirements.txt", requirements)
                    salva_debug_backup(f"{sha256.hexdigest()}/{cmdline0}", appfile)
                    salva_debug_backup(f"__config__{sha256.hexdigest()}__", json.dumps(cfg))

                    help0 = cmd_interface(["python", cmdline0, "-h" ], True)

                    posusage = False
                    posarguments = False
                    fimdescr2 = False
                    descricao2 = ''
                    largs = []
                    largs0 = {}
                    for line in help0.splitlines():


                        if posusage:
                            tmp = line.split(' ')
                            for x in tmp:
                                if x != '' and x != cmdline0:
                                    largs.append(x)
                            print(f"sintaxe argumentos: {largs}")
                            posusage = False

                        tst = re.search('.*Usage.*', line )
                        if tst:
                                posusage = True
                                fimdescr2 = True 


                        if not fimdescr2:
                            descricao2 += line

                        tst = re.search('.*Options.*', line )
                        if tst:
                            if posarguments:
                                posarguments = False    

                        if posarguments:

                            tmp = line.split(' ')
                            for x in tmp:
                                if x != '' and x != cmdline0:
                                    args0 = {}
                                    tmp = re.search(f'{x}(.*).default.(.*).', line )
                                    if tmp:
                                        args0['variavel'] = x
                                        args0['descricao'] = tmp.group(1).strip()
                                        if tmp.group(2).find("--") != -1:
                                            dargs = tmp.group(2).strip().split(' ')
                                            args0['cmd'] = dargs
                                            args0['default'] = ''
                                        # elif tmp.group(2).find("UUID") != -1:
                                        #     dargs = tmp.group(2).strip().split(' ')
                                        #     args0['cmd'] = ''
                                        #     args0['default'] = 'UUID'
                                        
                                        else:
                                            args0['cmd'] = []
                                            args0['default'] = x

                                        #print(f"largs0: {x} desc: {tmp.group(1)} default: {tmp.group(2)}")

                                    else:
                                        tmp = re.search(f'{x}(.*)', line )  
                                        if tmp: 
                                            args0['variavel'] = x
                                            args0['descricao'] = tmp.group(1).strip()
                                            args0['cmd'] = []
                                            args0['default'] = ''
                                            #print(f"largs0: {x} desc: {tmp.group(1)} ")

                                    largs0[x] = args0
                                    break

                            # for arg in largs:
                            #     tmp = re.search(f'{arg}(.*).default.(.*).', line )
                            #     if tmp:
                            #         print(f"arg: {arg} desc: {tmp.group(1)} default: {tmp.group(2)}")

                            #     else:
                            #         tmp = re.search(f'{arg}(.*)', line )  
                            #         if tmp: 
                            #             print(f"arg: {arg} desc: {tmp.group(1)} ")


                        tst = re.search('.*Arguments.*', line )
                        if tst:
                                posarguments = True   



                    rel = cmd_interface(["python", cmdline0, "-v" ], True)
                    appver0 = f"{appver} Cmd {rel}"

                    largs2 = []
                    for item in largs:
                        largs2.append(largs0[item])

                    cfg['id'] = sha256.hexdigest()
                    cfg['nome-app'] = nomeapp0
                    cfg['descricao-app'] = descapp0
                    cfg['cmd-app'] = cmdline0
                    cfg['url-app'] = pathdockerfile
                    try:
                        cfg['atualizacao-app'] = req.headers['Last-Modified'] ## comparar e tratar aqui versões antes de salvar em cfg['atualizacao-app']
                    except:
                        cfg['atualizacao-app'] = req.headers['ETag']
                    cfg['versao-app'] = appver0 
                    cfg['descricao-cmd-app'] = descricao2
                    cfg['args-app'] = largs2


                    print(f"largs: \n{largs}\n")
                    print(f"largs0: \n{largs0}\n")

                    data0 = datetime.now()
                    print(f'*** fim config geral {data0.strftime("%d/%m/%Y %H:%M:%S")}')



                data0 = datetime.now()
                print(f'*** fim config {data0.strftime("%d/%m/%Y %H:%M:%S")}')

                print(f"cfg: \n{str(json.dumps(cfg))}\n")




                await salva_offline('cfg.json', json.dumps(cfg)) # 
                await salva_offline(f"GuiApp.py_{fmtnome(cfg['atualizacao-gui'])}.bak", gui.text)
                await salva_offline(f"{cfg['id']}/dockerfile_{fmtnome(cfg['atualizacao-app'])}.bak", dockerfile)
                await salva_offline(f"{cfg['id']}/requirements.txt_{fmtnome(cfg['atualizacao-app'])}.bak", requirements)
                await salva_offline(f"{cfg['id']}/{cfg['cmd-app']}_{fmtnome(cfg['atualizacao-app'])}.bak", appfile)

                # debug e backup


                salva_debug_backup('cfg.json', json.dumps(cfg)) # 
                salva_debug_backup(f"GuiApp.py_{fmtnome(cfg['atualizacao-gui'])}.bak", gui.text)
                salva_debug_backup(f"{cfg['id']}/dockerfile_{fmtnome(cfg['atualizacao-app'])}.bak", dockerfile)
                salva_debug_backup(f"{cfg['id']}/requirements.txt_{fmtnome(cfg['atualizacao-app'])}.bak", requirements)
                salva_debug_backup(f"{cfg['id']}/{cfg['cmd-app']}_{fmtnome(cfg['atualizacao-app'])}.bak", appfile)

                # http://testando:13000/testes/ValidadorTributario/raw/master/Dockerfile
                # http://testando:13000/testes/ValidadorTributario/raw/master/requirements.txt

            except:
                print(f"Erro de conexão ao baixar configurações iniciais do App")
                if not await existe('cfg.json'):
                    exit()

        print(f"le_offline('cfg.json'): >{await le_offline('cfg.json')}<")

        cfg = json.loads(await le_offline('cfg.json'))


        appver = cfg['versao-app'] 
        nomeapp = cfg['nome-app']
        descapp = cfg['descricao-app']
        cmdline = cfg['cmd-app']
        txtwarn = cfg['descricao-cmd-app']



        data0 = datetime.now()
        print(f'*** fim config {data0.strftime("%d/%m/%Y %H:%M:%S")}')


    def button_clicked(e):
        debug(e.control.data)

    def sair_app(e):
        page.window_close()
        page.window_destroy()

    def le_json(key):

        value = ""
        path = logcmd_+"/"+key + ".json"
        print(f"le_json: {path}")

        try:
            with open(path, 'r', encoding="utf-8") as file:
                value = file.read()
        except:
            debug(f"erro le_json: {key}")    

        return value


    def escolhe_pasta(e):

        global  RepositorioAppLocal, args

        print(f"escolhe pasta (e.control.data): {(e.control.data)}")
        preparando('lendo conteudo')

        i = int(e.control.data) 
        


        def dir_clicked(e):
            global  RepositorioAppLocal
            preparando('lendo conteudo')
            args[i].data = e.control.data # e.control.data vem da escolha clickada
            args[i].value = f"file://{e.control.data}" # e.control.value vem da escolha clickada
            crialistagem()

            # dirok = True
            # try:            
            #     tmp = cmd_interface(["python", cfg['cmd-app'], '--dir' , args[i].data ], True) # os.listdir(e.control.data)
            # except:
            #     dirok = False 
            #     dlg = ft.AlertDialog( title=ft.Text("Sem permissão de acesso em '" + str(args[i].data) + "'"), on_dismiss=lambda e: debug("Dialog dismissed!") )   
            #     e.control.page.dialog = dlg
            #     dlg.open = True
            #     e.control.page.update()
                
            # if dirok:
            #     pastaRaiz= str(e.control.data)
            #     salva_offline('pastaRaiz', pastaRaiz) # page.client_storage.set("pastaRaiz", pastaRaiz) #salva_offline('pastaRaiz', pastaRaiz)
            #     escolhe_pasta(e)    


        def crialistagem():

            lv = ft.ListView(expand=True, spacing=2)

            print(f"*** >{args[i].data}<")

            # if pastaRaiz == "/":
            #     confirm = Container(ft.ElevatedButton(f"Confirmar pasta file://{str(args[i].data)} e voltar", disabled=True, icon=ft.icons.DONE_ALL,  on_click=pg_ini),)
            # else:
            #     confirm = Container(ft.ElevatedButton(f"Confirmar pasta file://{str(args[i].data)} e voltar", icon=ft.icons.DONE_ALL,  on_click=pg_ini),)

            confirm = Container(ft.ElevatedButton(f"Confirmar pasta file://{str(args[i].data)} e voltar", icon=ft.icons.DONE_ALL,  on_click=pg_ini),)

            filelist = cmd_interface(["python", cfg['cmd-app'], '--dir' , args[i].data  ], True)


            # if pastaRaiz == "/":
            #     try:
            #         raiz = os.listdrives()
            #         for entry in raiz:
            #             full_path =  str(Path(entry).as_posix()) 
            #             lv.controls.append(ft.ListTile(
            #                             leading=ft.Icon(ft.icons.FOLDER),
            #                             title=ft.Text(full_path ),
            #                             on_click=dir_clicked,
            #                             data= full_path,
            #                         ),)

            #     except:
            #         raiz = "/"
            # else:
            #     for entry in os.listdir(pastaRaiz):
            #         full_path = pastaRaiz + "/" + entry # os.path.join(pastaRaiz, entry) # full_path = pastaRaiz + "/" + entry 
            #         if os.path.isdir(full_path):
            #             lv.controls.append(ft.ListTile(
            #                             leading=ft.Icon(ft.icons.FOLDER),
            #                             title=ft.Text(str(Path(full_path).as_posix()) ),
            #                             on_click=dir_clicked,
            #                             data= str(Path(full_path).as_posix()),
            #                         ),) 

            path = Path(e.control.data)
            lv.controls.append(ft.ListTile(
                            leading=ft.Icon(ft.icons.FOLDER),
                            title=ft.Text(".."),
                            on_click=dir_clicked,
                            data= str(Path(str(args[i].data)).parent.absolute().as_posix()),
                        ),)    

            for entry in filelist['dirs']: # os.listdir(pastaRaiz):
                full_path = filelist['path'] + "/" + entry # os.path.join(pastaRaiz, entry) # full_path = pastaRaiz + "/" + entry 
                if True: # os.path.isdir(full_path):
                    lv.controls.append(
                                    ft.ListTile(
                                        leading=ft.Icon(ft.icons.FOLDER),
                                        title=ft.Text(str(entry) ),
                                        on_click=dir_clicked,
                                        data= str(Path(full_path).as_posix()),
                                    ),
                                ), 
            for entry in filelist['files']: # os.listdir(pastaRaiz):
                full_path = filelist['path'] + "/" + entry # os.path.join(pastaRaiz, entry) # full_path = pastaRaiz + "/" + entry 
                if True: # os.path.isdir(full_path):
                    lv.controls.append(
                                    ft.ListTile(
                                        disabled=True, 
                                        leading=ft.Icon(ft.icons.TEXT_SNIPPET_OUTLINED),
                                        title=ft.Text(str(entry) ),
                                        on_click=dir_clicked,
                                        data= str(Path(full_path).as_posix()),
                                    ),
                                ), 


            page.scroll = "none"
            page.clean()

            page.add(
                BarraSuperior("/ escolher origem"),
            )

            page.add(confirm)

            page.add(lv)

            page.add(
                BarraInferior(), 
            )

        crialistagem()


    # def pasta_inicial(e):
    #     if e.path is None: return
    #     global pastaRaiz, RepositorioAppLocal
    #     pastaRaiz = e.path




    def mailto_dlg(e):
        webbrowser.open(f'{link}{base64.b64decode(test+"==").decode("utf-8")}{nomeapp}&body=%0A%0A%0A%0A%0A%0A%0A%0A%0A%0AVers%C3%A3o%20e%20ambiente em uso:%0A%0A{nomeapp} {appver} %0A{sys.version}%0A{os.environ}%0A')
        dlg.open = False
        page.update()

    def close_dlg(e):
        dlg.open = False
        page.update()

    def open_dlg(e):
        page.dialog = dlg
        dlg.open = True
        page.update()


    def open_dlgupgrade(e):


        def upgrade(e):

            global cfg

            # voltar p/ shared preferences
            sha256 = hashlib.sha256()
            sha256.update(nomeappgui.encode('utf-8'))

            pathori = f"{RepositorioAppLocal}/{sha256.hexdigest()}"

            print(f"{RepositorioAppLocal}/{sha256.hexdigest()}/{fmtnome(os.path.basename(__file__))}_{fmtnome(cfg['atualizacao-gui'])}_{fmtnome(str(datetime.now()))}.bak")
            print(Path(__file__).parent.resolve())

            shutil.copy(f"{Path(__file__).parent.resolve()}/{os.path.basename(__file__)}", f"{RepositorioAppLocal}/{sha256.hexdigest()}/{fmtnome(os.path.basename(__file__))}_{fmtnome(cfg['atualizacao-gui'])}_{fmtnome(str(datetime.now()))}.bak") 

            shutil.copy( f"{Path(__file__).parent.resolve()}/{cfg['cmd-app']}" , f"{RepositorioAppLocal}/{sha256.hexdigest()}/{cfg['id']}/{cfg['cmd-app']}_{fmtnome(cfg['atualizacao-app'])}_{fmtnome(str(datetime.now()))}.bak" )

            # salva_offline(f"GuiApp.py_{fmtnome(cfg['atualizacao-gui'])}.bak", gui.text)
            # salva_offline(f"{cfg['id']}/dockerfile_{fmtnome(cfg['atualizacao-app'])}.bak", dockerfile)
            # salva_offline(f"{cfg['id']}/requirements.txt_{fmtnome(cfg['atualizacao-app'])}.bak", requirements)
            # salva_offline(f"{cfg['id']}/{cfg['cmd-app']}_{fmtnome(cfg['atualizacao-app'])}.bak", appfile)

            #shutil.copy( f"{RepositorioAppLocal}/{sha256.hexdigest()}/{cfg['id']}/{cfg['cmd-app']}" , f"{RepositorioAppLocal}/{sha256.hexdigest()}/{cfg['id']}/{cfg['cmd-app']}_{fmtnome(cfg['atualizacao-app'])}_{fmtnome(str(datetime.now()))}.bak" )

            page.window_close()
            page.window_destroy()


        def close_dlgupgrade(e):
            dlgupgrade.open = False
            page.update()

        dlgupgrade = ft.AlertDialog( 
            modal=True,
            title=ft.Text("Atualizações disponíveis !"),
            content=ft.Text(f"{platform.system()} {__file__} --> {cfg['versao-disponivel-gui']}\n\nVersão atualmente em uso: {cfg['atualizacao-gui']}.\n\nNova versão atualizada: {cfg['versao-disponivel-gui']}.\n\nSerá necessário reiniciar o aplicativo após a atualização."),
            actions=[
                ft.TextButton("Atualizar agora", on_click=upgrade),
                ft.TextButton("Mais tarde", on_click=close_dlgupgrade),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            on_dismiss=lambda e: debug("Modal dialog dismissed!"),) 
        
        page.dialog = dlgupgrade
        dlgupgrade.open = True
        page.update()

    dlg = ft.AlertDialog( 
        modal=True,
        title=ft.Text("Informações !"),
        content=ft.Text(txtwarn + "\n\n\nFuncionalidades do aplicativo: \n\n" + descapp),
        actions=[
            ft.TextButton("Reportar Erros e Sugestões", on_click=mailto_dlg),
            ft.TextButton("Ok", on_click=close_dlg),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
        on_dismiss=lambda e: debug("Modal dialog dismissed!"),)  



    # cnpj_ = ""
    # ano_ = ""
    # prest_ = ""

    # cnpj_ft = ft.TextField(label="CNPJ da matriz", value=cnpj_)
    # ano_ft = ft.TextField(label="Ano Fiscal", value=ano_)

    pbmsg = ft.Text(value="")
    pb = ft.ProgressBar(width=400)


    def BarraInferior():

        return ft.BottomAppBar(
                    bgcolor=ft.colors.BLUE,
                    shape=ft.NotchShape.CIRCULAR,
                    content=ft.Row(
                        controls=[
                            ft.Container(
                                expand=True ,
                                content=ft.Text(descapp + " - " + appver, color="white"),
                                ),
                        ]
                    ),
            )


    def BarraSuperior(title):

        title = nomeapp + " " + title
        ret = ""

        upgrade0 = ft.Text()
        if cfg['versao-disponivel-gui'] != "":
            upgrade0 = ft.IconButton(ft.icons.UPGRADE,icon_color="white", on_click=open_dlgupgrade)

        if re.search(r"elatório", title) :
            bars =    AppBar(
                leading=ft.Icon(ft.icons.ACCOUNT_BALANCE, color="white"),
                title=ft.Text(title , color="white"),
                center_title=False,
                bgcolor=ft.colors.BLUE,
                shape=ft.NotchShape.CIRCULAR,
                actions=[
                    ft.IconButton(ft.icons.ARROW_BACK_IOS,icon_color="white", on_click=view_pop),
                    #ft.IconButton(ft.icons.DOWNLOAD,icon_color="white", on_click=download), SYSTEM_UPDATE
                    ft.IconButton(ft.icons.INFO,icon_color="white", on_click=open_dlg),
                    upgrade0,
                    ft.IconButton(ft.icons.EXIT_TO_APP,icon_color="white", on_click=sair_app),
                ],
            )
        elif re.search(r"/", title) :
            bars =    AppBar(
                leading=ft.Icon(ft.icons.ACCOUNT_BALANCE, color="white"),
                title=ft.Text(title , color="white"),
                center_title=False,
                bgcolor=ft.colors.BLUE,
                shape=ft.NotchShape.CIRCULAR,
                actions=[
                    ft.IconButton(ft.icons.ARROW_BACK_IOS,icon_color="white", on_click=view_pop),
                    ft.IconButton(ft.icons.INFO,icon_color="white", on_click=open_dlg),
                    upgrade0,
                    ft.IconButton(ft.icons.EXIT_TO_APP,icon_color="white", on_click=sair_app),
                ],
            )
        else:
            bars =    AppBar(
                leading=ft.Icon(ft.icons.ACCOUNT_BALANCE, color="white"),
                title=ft.Text(title , color="white"),
                center_title=False,
                bgcolor=ft.colors.BLUE,
                shape=ft.NotchShape.CIRCULAR,
                actions=[
                    ft.IconButton(ft.icons.INFO,icon_color="white", on_click=open_dlg),
                    upgrade0,
                    ft.IconButton(ft.icons.EXIT_TO_APP,icon_color="white", on_click=sair_app),
                ],
            )    

        return bars 



    def view_pop(view):
        pg_ini(True)  

    def preparando(msg):  

        page.scroll = "none"
        page.clean()
        page.add(
            BarraSuperior(f"/ {msg}"),
            ft.Column(
                [ft.ProgressRing()],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
            BarraInferior(), 
        )
        page.update()

    def progresso():  

        page.scroll = "none"
        page.clean()
        page.add(
            BarraSuperior("/ progresso"),
            ft.Column(
                [ft.ProgressRing()],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
            ft.Column([ pbmsg, pb]),
            BarraInferior(), 
        )
        pb.value = 0.001
        page.update()


    def relatorio(idx):

        global fmt, arquivosenha,  RepositorioAppLocal, vpcnt, cntanon0, tcodec, cnpj_, ano_, prest_, log_, pathprestok

        #pathprestok = f"{log_}/RelatorioValidatorTributario_{re.sub(r"[^A-Za-z0-9_()]", "",cnpj_)}_{ano_}"
        # path = f"{logcmd_}/RelatorioValidatorTributario_{pathprestok}.md"
        # with open(path, 'r', encoding="utf-8") as file:
        #     resumo = file.read()

        #base64_str = b.decode('utf-8') base64.b64decode

        relhtml = base64.b64decode(cmd_interface(["python", cmdline, "--file", relanteriores['relatorios'][idx]['relatorio-html']['arquivo'] ], True)) 
        fmt =  json.loads(base64.b64decode(cmd_interface(["python", cmdline, "--file", relanteriores['relatorios'][idx]['dataframes-template'] ], True)))

        #fmt = json.loads(base64.b64decode(cmd_interface(["python", cmdline, "--file", relanteriores['relatorios'][idx]['dataframes-template'] ], True)))
        #fmt = base64.b64decode(cmd_interface(["python", cmdline, "--file", relanteriores['relatorios'][idx]['dataframes-template'] ], True)) 

        resumo = md(relhtml) # d.value

        #print(f"{fmt}")

        # path = f"{logcmd_}/RelatorioValidatorTributario_{pathprestok}.testpage.json"
        # with open(path, 'r', encoding="utf-8") as file:
        #     testpage = file.read()

        #text = ft.Text(value=relhtml, selectable=True)    

        def baixar(e):
            print(e.control.data)
            page.launch_url(e.control.data)

        def copy(e):
            pyperclip.copy(relhtml)

        b_dataframes = ft.Row()
        b_dataframes.controls.append(ft.ElevatedButton("Copiar", icon=ft.icons.CONTENT_COPY, on_click=copy))

        for b in relanteriores['relatorios'][idx]['dataframes']:
            b_dataframes.controls.append(ft.ElevatedButton(f"{b['descricao']}", icon=ft.icons.BACKUP_TABLE, data=f"{b['arquivo']}", on_click=resant))

        b_local = ""
        if host == "":
            b_local = ft.Row()


        page.scroll = "auto"
        page.clean()
        page.add(
            BarraSuperior(f"/ relatório {pathprestok}"), 
            ft.Column([
                b_dataframes ,
            ]), 
            ft.Markdown(
                resumo,
                extension_set=ft.MarkdownExtensionSet.GITHUB_WEB,
            ), 
            ft.Column([
                ft.Row(
                    [
                        # ft.ElevatedButton("Ver no navegador", icon=ft.icons.WEB,data= f"{str(Path(f"{logcmd_}/RelatorioValidatorTributario_{pathprestok}.html").as_uri())}" ,  on_click=baixar),
                        # ft.ElevatedButton("Baixar arquivos validados", icon=ft.icons.APPROVAL,data= f"{str(Path(pasta_ok).as_uri())}" ,  on_click=baixar),
                        # ft.ElevatedButton("Baixar arquivos não validados", icon=ft.icons.RESTORE_FROM_TRASH,data= f"{str(Path(pasta_nok).as_uri())}" ,  on_click=baixar),
                        # ft.ElevatedButton("Baixar arquivos originais", icon=ft.icons.WEB_STORIES,data= f"{str(Path(pastaRaiz).as_uri())}" ,  on_click=baixar), 
                    ]
                ),
            ]),
            BarraInferior(),
        )   

    def cmd_script(e):

        global relanteriores, arquivosenha, RepositorioAppLocal, vpcnt, cntanon0, tcodec, cnpj_, ano_, prest_, log_, pathprestok

        # if len(ano_ft.value) == 4 and int(ano_ft.value) > 2010:
        #     ano_ = ano_ft.value

        # c = ""
        # if pycpfcnpj.cnpj.validate(re.sub(r"[^0-9]", "", cnpj_ft.value)):
        #     c = re.sub(r"[^0-9]", "", cnpj_ft.value) # # pycpfcnpj.cpfcnpj.clear_punctuation(cnpj_ft.value)

        # #listatemporaria = []
        # if pastaRaiz == "/" or c == "" or ano_ == "" :
        #     page.scroll = "none"
        #     page.clean()
        #     page.add(
        #         BarraSuperior("/ erro"),
        #         Container(Text("Necessário selecionar a pasta raiz e preencher corretamente o CNPJ da matriz e o Ano Fiscal !", bgcolor="white",  expand=True)),
        #         ft.ElevatedButton("voltar", on_click=pg_ini),
        #         BarraInferior(), 
        #     )

        # cnpj_ = c

        # # salva_offline('cnpj',cnpj_)
        # # salva_offline('ano',ano_)
        # # salva_offline('prest',prest_)


        # debug(e)

        # #threading.Thread(target=f, args=(arg1, arg2)).start()
        # # python validadorTributario202409160000.py 
        # cmd_args = '/home/sergiosq/teletrabalho/tmp/trib/in 00497373003216 2022  /home/sergiosq/teletrabalho/tmp/trib/out descrição opcional' 


        # # with open(f"{logcmd_}/progresso.log", 'a', encoding="utf-8") as file:
        # #     file.write( "\nIniciando: Descompactando e contando arquivos\n")


        # # debug(f"python {cmdline} {pastaRaiz} {cnpj_} {ano_} {logcmd_}")

        # remove_offline('res_brutos')
        # remove_offline('expandida')
        # remove_offline('filtrada')

        # print(f"python {cmdline} {pastaRaiz} {cnpj_} {ano_} ")
        # # debug(f"python {cmdline} {pastaRaiz} {cnpj_} {ano_} {logcmd_}")



        progresso()

        cmd = []
        cmd.append("python")
        cmd.append(cmdline)
        for item in args:
            if item.data:
                cmd.append(item.data)
            else:    
                cmd.append(item.value)

        salva_offline('ultimo_processamento', json.dumps(cmd) )  
        # debug e backup
        salva_debug_backup('ultimo_processamento', json.dumps(cmd) )       


        cmd_interface(cmd, False) #  cmd_interface(cmd, False) 
        
        last_line = " "
        while last_line.find("Concluido Relatorio ") == -1 and last_line.find("Erro: ") == -1 and last_line.find("Traceback") == -1 : # Traceback

            rel = cmd_interface(["python", cmdline, "--log", iduuid ], True)

            last_line = rel['tail']

            r1 = re.findall(r"([0-9]{1,2})%",last_line)
            timeout=1000
            if r1:
                pb.value = int(r1[0]) / 100
                pbmsg.value = last_line
                page.update()
            elif last_line != "\n" :
                pbmsg.value = last_line
                page.update() 

        pbmsg.value = "Carregando relatório ... "
        page.update()      
        relanteriores = cmd_interface(["python", cfg['cmd-app'], "--relatorios" ], True) 
        pathprestok = last_line[20:]
        salva_offline("pathprestok",pathprestok)
        debug(pathprestok)
        #config()
        relatorio(0)


    # trata resultados anteriores
    def resant(e):

        page.scroll = "none"
        page.clean()
        page.add(
            BarraSuperior("/ carregando"),
            ft.Column(
                [ft.ProgressRing(), ft.Text("Processando e formatando resultados: A saida ainda não está otimizada e poderá demorar um pouco para completar após o relógio.")],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
            BarraInferior(), 
        )
        page.update()

        stringo = base64.b64decode(cmd_interface(["python", cmdline, "--file", e.control.data ], True)).decode('utf-8')

        print(f"e.control.data: {e.control.data} stringo: ")

        dtfatual = pd.read_json(StringIO(stringo), orient='records')

        #dtfatual = base64.b64decode(cmd_interface(["python", cmdline, "--file", relanteriores['relatorios'][idx]['dataframes-template'] ], True)) 


        def click_tabela(e):
            print(f"click_tabela: {e.control.data}")
            

        def click_tabelaxxx(e):

            dlg = ft.AlertDialog( title=ft.Text("Detalhes: \n\n" + str(msg) + ""), scrollable=True, on_dismiss=lambda e: debug("Dialog dismissed!") )   
            e.control.page.dialog = dlg
            dlg.open = True
            e.control.page.update()


        ### cabeçalhos

        ### início funciona genérico
        def headers(dtf,fmt):
            global colscroll
            items=[]
            nc = -1
            for header in dtf.columns:
                nc = nc + 1
                if ( nc == 0 ) | (nc >= colscroll):
                    # adicionando os containers na lista

                    try:
                        width = int(fmt[header]['width'])
                    except:
                        width = 80

                    try:
                        txtheader = fmt[header]['txt']
                    except:
                        txtheader = header

                    items.append(
                        ft.Container(
                            content=ft.Text(value=txtheader),
                            alignment=ft.alignment.center,
                            width=width,
                            height=80,
                            bgcolor=ft.colors.BLUE,# bgcolor=ft.colors.INDIGO_100,
                            border_radius=ft.border_radius.all(5),
                            on_click= click_tabela,
                            data=header,
                        )
                    )


            return items              
        ### fim funcioona genérico

        def on_column_scroll(e: ft.OnScrollEvent):

            global linscroll

            # print(
            #     f"linscroll: {linscroll} Type: {e.event_type}, pixels: {e.pixels}, min_scroll_extent: {e.min_scroll_extent}, max_scroll_extent: {e.max_scroll_extent}"
            # )

            # if e.pixels == 0:
            #     linscroll = linscroll - 8

            # elif ((e.max_scroll_extent / e.pixels) < 2 ):
            #     linscroll = linscroll + 8
                
            # elif ((e.max_scroll_extent / e.pixels) > e.pixels ):
            #     linscroll = linscroll - 8


            # if linscroll < 0:
            #     linscroll = 0

            # if linscroll != 0:

            #     desenhap()



        ### início funcioona genérico
        
        def rows(dtf,fmt):


            global linscroll, colscroll
            
            lvf = ft.ListView(expand=True, spacing=2)

            if linscroll != 0:
                lvf.controls.append(
                                ft.Row(
                                    [
                                        ft.IconButton(icon=ft.icons.KEYBOARD_ARROW_UP_OUTLINED, on_click=scroll_col, data=[linscroll - 1,colscroll]),
                                        ft.IconButton(icon=ft.icons.KEYBOARD_DOUBLE_ARROW_UP_OUTLINED, on_click=scroll_col, data=[linscroll - 8,colscroll]),
                                    ]
                                ),
                            )

            nl = -1
            for index, row in dtf.iterrows():
                nl = nl + 1
                items = []
                nc = -1

                if (nl >= linscroll) & ( (nl - 16 ) <= (linscroll) ):
                    for header in dtf.columns:
                        nc = nc + 1
                        if ( nc == 0 ) | (nc >= colscroll):

                            try:
                                width = int(fmt[header]['width'])
                            except:
                                width = 80

                            try:
                                if (row[header]) >= 1:
                                    bgcolor = fmt[header]["1"]
                                elif (row[header]) == 0:
                                    bgcolor = fmt[header]["0"]
                                else:
                                    
                                    bgcolor = fmt[header]["-1"]  
                            except:
                                bgcolor = '' # bgcolor = '0xeeeeee' 

                            items.append(
                                ft.Container(
                                    content=ft.Text(value=row[header]),
                                    alignment=ft.alignment.center,
                                    width=width,
                                    height=80,
                                    bgcolor=bgcolor,
                                    border_radius=ft.border_radius.all(5),
                                    on_click= click_tabela,
                                    data=[row[header],header,index],
                                )
                            )


                    # criando a row a partir das colunas items
                    row = ft.Row(spacing=5, controls=items)
                    lvf.controls.append( row,) 

            lvf.controls.append(
                            ft.Row(
                                [
                                    ft.IconButton(icon=ft.icons.KEYBOARD_ARROW_DOWN_OUTLINED, on_click=scroll_col, data=[linscroll + 1,colscroll]),
                                    ft.IconButton(icon=ft.icons.KEYBOARD_DOUBLE_ARROW_DOWN_OUTLINED, on_click=scroll_col, data=[linscroll + 8,colscroll]),
                                ]
                            ),
                        )

            return lvf               
        ### fim funcioona genérico



        def scroll_col(e):

            global linscroll, colscroll

            print(f"scroll_ e.control.data: {e.control.data}")
            linscroll = e.control.data[0]
            colscroll = e.control.data[1]
            if colscroll > 32:
                colscroll = 32
            if colscroll < 0:
                colscroll = 0
            if linscroll < 0:
                linscroll = 0
            desenhap()


        def desenhap():


            global linscroll

            rowtop = ft.Row(spacing=5, controls=headers(dtfatual,  fmt))

            lv = ft.ListView(expand=True, spacing=2, on_scroll=on_column_scroll,)

            linhas = rows(dtfatual, fmt)
            lv.controls.append( linhas)


            page.scroll = "None"
            page.clean()
            page.add(
                BarraSuperior(f"/ relatório (Tabela {e.control.data})"),
            )    

            page.add(
                ft.Row(
                    [
                        ft.IconButton(icon=ft.icons.KEYBOARD_DOUBLE_ARROW_LEFT_OUTLINED, on_click=scroll_col, data=[linscroll,colscroll - 8]),
                        ft.IconButton(icon=ft.icons.KEYBOARD_ARROW_LEFT_OUTLINED, on_click=scroll_col, data=[linscroll,colscroll - 1]),
                        ft.IconButton(icon=ft.icons.KEYBOARD_ARROW_RIGHT_OUTLINED, on_click=scroll_col, data=[linscroll,colscroll + 1]),
                        ft.IconButton(icon=ft.icons.KEYBOARD_DOUBLE_ARROW_RIGHT_OUTLINED, on_click=scroll_col, data=[linscroll,colscroll + 8]),
                    ]
                ),
            )

            page.add(rowtop)

            page.add(lv)


            page.add(
                BarraInferior(),
            )  

        desenhap()


    def pg_ini(e):
        
        global relanteriores, iduuid, args, descapp, nomeapp, RepositorioAppLocal, pasta_in, cntanon, cntanon0, ddeep,  fmt, tipos, estados, meses, meserr, dtfatual, vpath, data_, resultadosexistentes, pathprestok

        cntanon = 0
        cntanon0 = 0


        preparando('atualizando relatórios')
        config()
        atualizacoes()
        relanteriores = cmd_interface(["python", cfg['cmd-app'], "--relatorios" ], True) # 
        salva_debug_backup('debug_anteriores.json', json.dumps(relanteriores)) # debug

        print(f"*** cfg: \n {cfg}")

        data0 = datetime.now()
        print(f'*** iniciando renderização da página inicial {data0.strftime("%d/%m/%Y %H:%M:%S")}')

        page.clean()
        page.scroll = "auto"
        page.add(
            BarraSuperior(""), 
        )

        if len(relanteriores['relatorios']) > 0:

            def dropdown_changed(e):
                page.update()
                preparando(f"Carregando {d.value}")
                print(f"*** dropdown: {e.control.data} {d.value}")
                idx = -1
                for i in relanteriores['relatorios']:
                    idx = idx + 1
                    if i['descricao'] == d.value:
                        print(f"idx: {idx} {relanteriores['relatorios'][idx]['relatorio-html']['arquivo']}")
                        break

                  

                page.update()
                relatorio(idx)


            d = ft.Dropdown(
                hint_text = "Escolher e Visualizar um resultado já existente",
                label = "Escolher e Visualizar um resultado já existente",
                on_change=dropdown_changed,
                )

            for item in relanteriores['relatorios']:
                d.options.append(ft.dropdown.Option(f"{item['descricao']}"))
                # d.value = option_textbox.value
                # option_textbox.value = ""

            page.add(
                    ft.Text("Resultados anteriores: ", weight=ft.FontWeight.BOLD) ,
                ) 

            page.add(
                ft.Container(
                    margin=20,
                    content=d
                )
            )


        page.add(
                    ft.Text("Novo Relatório: ", weight=ft.FontWeight.BOLD) ,
                ) 

        # inicializa - passar p/ config
        if len(args) == 0:

            print(f"# inicializa - passar p/ config")
            itemcmd = []
            i = 0
            for item in cfg['args-app']:

                if item['default'].find("UUID") == -1:

                    if len(relanteriores['relatorios']) > 0 :
                        value = relanteriores['args-last'][i]
                    else:
                        value = ""   
                    
                    if value == "":
                        value = item['default']

                    if len(item['cmd']) == 0:
                        args.append(
                            ft.TextField(label=f"{item['descricao']}", value=f"{value}")
                        )
                        page.add(
                            ft.Container(
                                    margin=20,
                                    content = args[i],
                            ) 
                        )
                    else:
                        
                        itemcmd.append(item['cmd'][0]) # --dir ?
                        itemcmd.append(value)
                        tmp = cmd_interface(["python", cfg['cmd-app'], itemcmd[0] , itemcmd[1] ], True)
                        texto = ft.Text()
                        args.append(
                                ft.TextField(label=f"{item['descricao']}", expand=True, read_only=True, data=f"{i}", value=f"file://{tmp['path']}")
                        )
                        args[i].data =  tmp['path']
                        print(f"args[i].data: {args[i].data} i: {i}")
                        page.add(
                            ft.Container(
                                    margin=10,
                                    content = ft.CupertinoButton(
                                                    content=ft.Row(
                                                        [
                                                            ft.Icon(name=ft.icons.FOLDER_OPEN_OUTLINED, color="blue"),
                                                            args[i],
                                                        ],
                                                    ),
                                    
                                    on_click=escolhe_pasta,
                                    data=f"{i}",
                                )
                            ) 
                        )

                    #page.add(args[i])

                    i = i + 1

                else:
                    # trata UUID sem adicionar ao form
                    id = uuid.uuid1()
                    iduuid = str(id.hex)
                    args.append( ft.TextField(data=f"{iduuid}", value=f"{iduuid}") )   
      
        else:

            itemcmd = []
            i = 0
            for item in cfg['args-app']:

                if item['default'].find("UUID") == -1:

                    if len(item['cmd']) == 0:
                        page.add(
                            ft.Container(
                                    margin=20,
                                    content = args[i],
                            ) 
                        )
                    else:
                        page.add(
                            ft.Container(
                                    margin=10,
                                    content = ft.CupertinoButton(
                                                    content=ft.Row(
                                                        [
                                                            ft.Icon(name=ft.icons.FOLDER_OPEN_OUTLINED, color="blue"),
                                                            args[i],
                                                        ],
                                                    ),
                                    
                                    on_click=escolhe_pasta,
                                    data=f"{i}",
                                )
                            ) 
                        )

                    i = i + 1

                else:
                    # sobreescrever
                    id = uuid.uuid1()
                    iduuid = str(id.hex)
                    args[i] = ft.TextField(data=f"{iduuid}", value=f"{iduuid}")  

            print(f"*** args: {args}")    
        
        page.add(
            ft.Container(
                margin=20,
                content = ft.ElevatedButton("Processar e Gerar Relatório", icon=ft.icons.DONE_ALL, on_click=cmd_script)
            )   
        )

        page.add(    
            BarraInferior(),
        )   
        page.update()





    global nomeapp, appver , resultadosexistentes #, cnpj_, ano_, prest_



    


    page.title = "Acessando Repositórios e Iniciando App atual ..."

    page.add(
        ft.ProgressRing(),
        ft.Text("Acessando Repositórios e Iniciando App atual ..."),
    )
    page.update()

    await config()

    #atualizando 
    dlg = ft.AlertDialog( 
        modal=True,
        title=ft.Text("Informações !"),
        content=ft.Text(txtwarn + "\n\n\nFuncionalidades do aplicativo: \n\n" + descapp),
        actions=[
            ft.TextButton("Reportar Erros e Sugestões", on_click=mailto_dlg),
            ft.TextButton("Ok", on_click=close_dlg),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
        on_dismiss=lambda e: debug("Modal dialog dismissed!"),) 

    page.title = nomeapp

    pg_ini(True)


if sshenv == "":
    ft.app(target=main) # GUI nativa do SO onde está executando
else:
    ft.app(target=main, view=ft.AppView.WEB_BROWSER, port=8080) # redireciona interface para http://127.0.0.1:8080/


