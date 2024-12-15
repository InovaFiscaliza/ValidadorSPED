#!/usr/bin/env python
# -*- coding: utf-8 -*-
import flet as ft
from flet import AppBar, ElevatedButton, Page, Text, View, colors,  Column, Container, Row
from markdownify import markdownify as md
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
from time import sleep
import re
import json
import paramiko
from datetime import datetime
import hashlib
import requests
import pyperclip

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

####################################################################
# pip install PyInstaller
# flet pack GuiApp.py
# flet pack ValidadorTributario.py
# split -d  -b 64m GuiApp.exe GuiApp.exe-
# split -d  -b 64m ValidadorTributario.exe ValidadorTributario.exe-
# flet pack instalar.py
# mv instalar.exe GuiApp.exe
####################################################################





atualizador = "atualizar"
if platform.system() == "windows":
   atualizador += ".exe"    
arquivo = "GuiApp"
guiapplink = "https://raw.githubusercontent.com/InovaFiscaliza/RepositorioFerramentasGRs/refs/heads/main/ValidadorTributario/dist/GuiApp" # "http://testando:13000/testes/ValidadorTributario/raw/master/GuiApp.py"
if platform.system() == "windows":
   guiapplink += ".exe"  


nomeapp = "Instalando"
nomeappgui = nomeapp



def main(page: ft.Page):

    pbmsg = ft.Text(value="")
    pb = ft.ProgressBar(width=400)

    def progresso():  

        page.scroll = "none"
        page.clean()
        page.add(
            ft.Column(
                [ft.ProgressRing()],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
            ft.Column([ pbmsg, pb]),
        )
        pb.value = 0.01
        page.update()

    def instalaGuiApp():


        
        progresso()

        if os.path.exists(f"{arquivo}.part"):
            os.remove(f"{arquivo}.part")

        part = -1
        while True:

            part = part + 1
            s = str(part)

            print(f"{guiapplink}-{s.zfill(2)}")




            response = requests.get(f"{guiapplink}-{s.zfill(2)}", stream=True)
            response.raw.decode_content = True

            totalbits = 0
            if response.status_code == 200:
                with open(f"{arquivo}.part", 'ab') as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            totalbits += 65536
                            #print(f"Parte {s.zfill(2)}:  {int(totalbits / 680000)} % ...")
                            pb.value = int(totalbits / 680000) / 100
                            pbmsg.value = f"Acessando Repositórios e instalando versão atual:\n\nEtapa {part + 1} -> {int(totalbits / 680000)} % concluida ..."
                            page.update()

                            f.write(chunk)

            elif ( response.status_code == 404 ) & ( part > 0 ):
                
                # salva se é o instalador
                if os.path.exists(arquivo) :
                    if os.path.getsize(arquivo) < 100000000  :
                        os.rename(arquivo, atualizador)
                

                if os.path.exists(arquivo):
                    os.rename(arquivo, f'arquivo.backup{datetime.now().strftime("Y%m%d%H%M%S")}')               
                os.rename(f"{arquivo}.part", arquivo)




                page.clean()
                page.add(
                    ft.Text("Instalação Concluida:\n\nNecessário reiniciar o aplicativo."),
                )
                page.update()

            else:
                print(f"part: {part}")
                page.clean()
                page.add(
                    ft.Text(f"Erro de conexão ou arquivo inexistente (Erro {response.status_code}): tente novamente mais tarde"),
                )
                page.update()
                break

     
 

    page.title = "Acessando Repositórios e instalando versão atual ..."

    page.clean()
    page.add(
        ft.Text("Acessando Repositórios e instalando versão atual ..."),
    )
    page.update()


    instalaGuiApp()


if "SSH_CLIENT" in os.environ:
    ft.app(target=main, view=ft.AppView.WEB_BROWSER, port=8080) # redireciona interface para http://127.0.0.1:8080/
else:
    ft.app(target=main) # GUI nativa do SO onde está executando


