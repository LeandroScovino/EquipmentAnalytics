from tkinter import *
from tkinter.filedialog import askdirectory
from tkinter.filedialog import askopenfilename
from tkinter.messagebox import askyesno, showerror, showinfo
import engine
import webbrowser
import os

def getFiles(path):
    returnFiles = []
    for file in os.listdir(path):
        if file.endswith(".csv"):
            returnFiles.append(file)
    return returnFiles

def main():
    class Window(Frame): 

        def __init__(self, master=None):
            Frame.__init__(self, master)
            self.master = master
            master.minsize(width=300, height=250)
            self.pack_propagate(0)
            self.init_window()

        def init_window(self):
            self.master.title('Tratar Dados RFV 2.0')
            self.pack(fill=BOTH, expand=1)

            # Enter Path
            self.pathText = Entry(self)

            # Botões
            convert = Button(self, text='Executar', command=self.convertFiles)
            history = Button(self, text='Log de Motores', command=self.history)
            events = Button(self, text='Log de Eventos', command=self.events)
            ts = Button(self, text='Arquivo de Troubleshoot', command=self.ts)
            dest = Button(self, text='Pasta de Destino', command=self.dest)

            #checkboxes
            global i 
            i = IntVar()
            c1 = Checkbutton(self, text='Debug?',variable=i)
            c1.pack()
            #i.get()

            # Campo de entrada
            history.place(relx=0.30, rely=0.20, anchor=CENTER)
            events.place(relx=0.70, rely=0.20, anchor=CENTER)
            ts.place(relx=0.50, rely=0.60, anchor=CENTER)
            c1.place(relx=0.70, rely=0.40, anchor=CENTER)
            dest.place(relx=0.30, rely=0.40, anchor=CENTER)
            
            
            # Botão de conversão
            convert.place(relx=0.50, rely=0.85, anchor=CENTER)

        def convertFiles(self):
            path = self.pathText.get()
            global ts_file
            ts_file = self.ts_file 
            global engine_file
            engine_file = self.egfile
            global event_file
            event_file = self.evfile
            global deb
            deb = i.get()
            if not os.path.exists(path):
                showerror('Erro! Pasta de destino inválida!',
                          'Escolha a pasta de destino desejada para continuar')
                # self.pathText.delete(0, END)
            else:
                if not engine_file:
                    showerror('Erro: Arquivo de motores não encontrado!',
                              'Selecione o *.xlsx com os dados dos motores do cliente antes de continuar')
                if not event_file:
                    showerror('Erro: Arquivo de eventos não encontrado!',
                              'Selecione o *.xlsx com os dados dos eventos do cliente antes de continuar')
                if not ts_file:
                    showerror('Erro: Arquivo de troubleshoot não encontrado!',
                              'Selecione o *.csv com a tabela de troubleshoot antes de continuar')
                                    
                else:
                    convertEngs = engine.preplistas(engine_file,event_file,ts_file,path,deb)

                    if not convertEngs:
                        info = showinfo(
                            'Sucesso!','Arquivos convertidos!')
                        # self.pathText.delete(0, END)
                    else:
                        info = showinfo(
                            'Erro!','Houve um erro! \nVerifique os arquivos e tente novamente!')

        def dest(self):
            dirname = askdirectory(parent=root,
                                   title='Selecione a pasta de destino')
            if dirname:
                print('Pasta de Destino:', str(dirname))
                self.pathText.insert(0, dirname)

        def history(self):
            enginefilename = askopenfilename(parent=root,
                                   title='Selecione o arquivo de log de motores', filetypes = [("Zip files","*.zip"),("Excel files","*.xlsx")])
            if enginefilename:
                self.egfile = enginefilename
                print('Arquivo de Parametros Históricos:', str(enginefilename))
				
        def events(self):
            eventfilename = askopenfilename(parent=root,
                                   title='Selecione o arquivo de log de eventos', filetypes = [("Excel files","*.xlsx")])
            if eventfilename:
                self.evfile = eventfilename
                print('Arquivo de Eventos:', str(eventfilename))

        def ts(self):
            ts_filename = askopenfilename(parent=root,
                                   title='Selecione o arquivo de Troubleshoot', filetypes = [("csv files","*.csv")])
            if ts_filename:
                self.ts_file = ts_filename
                print('Arquivo de Troubleshoot:', str(ts_filename))

    root = Tk()
    def callback(event):
        webbrowser.open_new('https://github.com/LeandroScovino')

    #logo sotreq
    scriptpath = os.path.dirname(os.path.realpath(__file__))

    lbl = Label(root, text=r"v4.2 - Agosto 2022 - Sobre / Ajuda", fg="blue", cursor="hand2")
    lbl.pack(side='bottom')
    lbl.bind("<Button-1>", callback)

    root.geometry('300x250')
    app = Window(root)
    root.mainloop()

if __name__ == '__main__':
    main()
