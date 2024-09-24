# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:43:52 2022

@author: SatishVenkataraman
"""
import tkinter as tk
from tkinter import W,E,N,S, IntVar,ttk, TclError, Frame, Button, TOP, messagebox
from tkinter.ttk import Combobox, Style
import QuoteProcessingOrchestrator as qpo
#import distutils
from distutils import util
import time
from tktooltip import ToolTip #https://pypi.org/project/tkinter-tooltip/
import commonUtils
from commonUtils import extn_utils as eu

class iqGUIApp(tk.Tk):
	def __init__(self, parent):
		self.parent = parent
		self.selected_conversionScope=tk.StringVar()
		self.selected_conversion = tk.StringVar()
		self.selected_copy = tk.StringVar()
		self.selected_storedProc = tk.StringVar()
		self.selected_dbtogs = tk.StringVar()
#		self.frm = ttk.Frame(root, padding=10, style='My.TFrame')
		self.frm = Frame(root, background=lightGreenColor)
		self.frm.grid(sticky= "we")
		# Make the frame sticky for every case
		self.frm.grid_rowconfigure(0, weight=1)
		self.frm.grid_columnconfigure(0, weight=1)
		
		# Make the window sticky for every case
		root.grid_rowconfigure(0, weight=1)
		root.grid_columnconfigure(0, weight=1)
		self.frm.pack(fill=tk.BOTH, expand=True)
		self.setup()

	def processButtonHandler(self):
		self.selected_conversionScope = self.selected_conversionScope.get()
		self.selected_conversion = bool(util.strtobool(self.selected_conversion.get()))
		self.selected_copy = bool(util.strtobool(self.selected_copy.get()))
		self.selected_storedProc = bool(util.strtobool(self.selected_storedProc.get()))
		self.selected_dbtogs = bool(util.strtobool(self.selected_dbtogs.get()))
		userVariables = { 'scope_complete_partial' : self.selected_conversionScope , 'isConversionNeeded' : self.selected_conversion, 'isCopyNeeded': self.selected_copy , 'isRunStoredProcNeeded': self.selected_storedProc, 'isUploadDBTOGSNeeded':self.selected_dbtogs}

#		print(userVariables)
#			os.system('python test2.py LoadAndProcess {userVariables}')
		etl = qpo.QuoteProcessingOrchestrator("LoadAndProcessQuotes", userVariables)
		etl.setup()
		etl.loadAndProcessQuotes()
		messagebox.showinfo("showinfo", "Completed Processing !")
		if self.frm:   # This code is to get back to the same default options
			self.frm.destroy()
			iqGUIApp.__init__(self,root)


	def setup(self):
#		frm = ttk.Frame(root, padding=10)
		self.frm.grid()

		ttk.Label(self.frm, text="INSTAQUOTE PROCESSOR", background='#EAFAF1', font=("Arial", 20)).grid(column=0, row=0,padx=5, pady=5, columnspan = 2)
		lbl_processingScope = ttk.Label(self.frm, text="Processing Scope", background='#EAFAF1')
		lbl_processingScope.grid(column=0, row=1,padx=5, pady=5,sticky=W)
		self.selected_conversionScope.set("Partial")

		frame2 = Frame(self.frm, background=lightGreenColor)
		frame2.grid()		
		rdo_complete = ttk.Radiobutton(frame2, text="Complete", variable=self.selected_conversionScope, value="Complete", style = 'Scope.TRadiobutton')
		rdo_complete.grid(column=0, row=1)
		rdo_partial = ttk.Radiobutton(frame2, text="Partial", variable=self.selected_conversionScope, value="Partial", style = 'Scope.TRadiobutton')
		rdo_partial.grid(column=1, row=1)
		frame2.grid(column=1, row = 1)
		
		yesNoData=("Yes", "No")
		lbl_processQuotes = ttk.Label(self.frm, text="Process Quotes ?", background='#EAFAF1')
		lbl_processQuotes.grid(column=0, row=2,padx=5, pady=5,sticky=W)

		var_convert = ttk.Combobox(self.frm, height=5, values=yesNoData, textvariable=self.selected_conversion)
		var_convert.grid(column=1, row=2,padx=5, pady=5)
		var_convert.current(0)
		
		lbl_copy = ttk.Label(self.frm, text="Copy ?", background='#EAFAF1')
		lbl_copy.grid(column=0, row=3,padx=5, pady=5,sticky=W)
		var_copy = ttk.Combobox(self.frm, height=5, values=yesNoData, textvariable=self.selected_copy)
		var_copy.grid(column=1, row=3,padx=5, pady=5)
		var_copy.current(0)
		
		lbl_runSP = ttk.Label(self.frm, text="Run Stored Proc?", background='#EAFAF1')
		lbl_runSP.grid(column=0, row=4,padx=5, pady=5,sticky=W)
		var_runstoredproc = ttk.Combobox(self.frm, height=5, values=yesNoData, textvariable=self.selected_storedProc)
		var_runstoredproc.grid(column=1, row=4,padx=5, pady=5)
		var_runstoredproc.current(0)
		
		lbl_toStaging = ttk.Label(self.frm, text="DB to Staging Sheet ?", background='#EAFAF1')
		lbl_toStaging.grid(column=0, row=5,padx=5, pady=5,sticky=W)
		var_dbtogs = ttk.Combobox(self.frm, height=5, values=yesNoData, textvariable=self.selected_dbtogs)
		var_dbtogs.grid(column=1, row=5,padx=5, pady=5)
		var_dbtogs.current(0)
		
		Button(self.frm, text="Run", command=self.processButtonHandler).grid(column=0, row=7, padx=20, pady=20,  sticky='nesw')
#		btn_quit = ttk.Button(self.frm, text="Quit", command=root.destroy).grid(column=2, row=7,padx=5, pady=5)
		btn_quit = Button(self.frm, text="Quit", command=root.destroy).grid(column=1, row=7,padx=20, pady=20,  sticky = 'nesw')
		#Bind tooltips with various labels
		eu.createToolTip(lbl_processingScope, "Complete will clean out the database while Partial will replace/add for just the vendors who have Yes and retain the other vendors")
		eu.createToolTip(lbl_processQuotes, "Ingest, clean quotes and create files on local")
		eu.createToolTip(lbl_copy, "Copy files from local to BOD SQL Server")	
		eu.createToolTip(lbl_runSP, "Run stored procedure to process created files an apply  logic to award against standing orders")
		eu.createToolTip(lbl_toStaging, "Get award details to Google Sheet for RPA BOT")
		

if __name__ == "__main__":
	root=tk.Tk()
	root.title('Instaquote Processor GUI')
	root.geometry("400x300+10+10")
	root.resizable(0,0)
	try:
        # windows only (remove the minimize/maximize button)
		root.attributes('-toolwindow', True)
		#Make the window jump above all
		root.attributes('-topmost',True)
	except TclError:
		print('Not supported on your platform')
	lightGreenColor = '#EAFAF1'                 # Its a light green color
	root.configure(bg=lightGreenColor)          # Setting color of main window to myColor

	s1 = Style()
	s1.configure('My.TFrame', background=lightGreenColor)
	s2 = ttk.Style()                     # Creating style element
	s2.configure('Scope.TRadiobutton',    # First argument is the name of style. Needs to end with: .TRadiobutton
        background=lightGreenColor,         # Setting background to our specified color above
        foreground='black') 
	
	app = iqGUIApp(root)
	root.config(bg='#EAFAF1')
	root.mainloop()