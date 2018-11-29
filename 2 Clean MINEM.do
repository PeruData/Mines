*************************************************************************
* Peruvian Mining Production dataset - Part 2
* Sebastian Sardon
* Last updated: Dec 2017
* Creates mining production dataset using output of 'spreadsheets to dta'
* Reference period: 2002-2017
*Note: takes as input prices from "Mining Prices.do" [00_Data, Ketchup]
*************************************************************************

cap restore
clear all 
set more off
global ccc "/Users/Sebastian/Documents/Papers/Mines SSB/00_Data"
cd "$ccc"

local minerals COPPER GOLD IRON LEAD SILVER TIN ZINC

use "Mines/Production/in/production_raw.dta", clear

*0 Translate
	replace mineral = "COPPER" if mineral == "COBRE"
	replace mineral = "GOLD"   if mineral == "ORO"
	replace mineral = "IRON"   if mineral == "HIERRO"
	replace mineral = "LEAD"   if mineral == "PLOMO"
	replace mineral = "SILVER" if mineral == "PLATA"
	replace mineral = "TIN"    if mineral == "ESTANHO"
	replace mineral = "ZINC"   if mineral == "ZINC"
	rename titular firm
	rename unidad mine
	rename departamento dep
	rename provincia prov
	rename distrito dist
	

*1. get USD value of production
	merge m:1 mineral year using "Prices/prices.dta", keep(1 3)
	
	*Harmonize units in prices and quantities are congruent:
		*Gold price from oz to g
		replace price = price/31.1 if mineral == "GOLD"
		*Silver price from oz to kg
		replace price = 1000*price/31.1 if mineral == "SILVER"
		
	gen double value_MM = prod_quant*price/1000000 
	
	*This next line is used to avoid double-counting (else totals inconsistent with official documents)
        keep if concentracion==1
    gsort -value_MM
    
*2. Clean "titular" variable (company name):
	gen firm1=firm
	
	*2.1 Drop prefixes and suffixes
		*Remove whitespace (restored after the block)
		replace firm1=subinstr(firm1," ","_",.)
		local prefixes  AURIFERA_ COMPANHIA_ DE_MINAS_ DE_RECURSOS_LINCEARES_ EMPRESA_
		local prefixes `prefixes' MINERA_  SOCIEDAD_ S.M.R.L._                                   
		local suffixes _ANDINA_PERU _EN_MARCHA _EN_LIQUIDACION _E.I.R.L. _LTDA.
		local suffixes `suffixes' _DEL_PERU_S.A.A.  _S.A.C.  _S.A.A. _S._A. _S.A. _S.A  _S_A
		local suffixes `suffixes' _S.C.R.L. _S.R.L. _SUCURSAL
		local trash_strings `prefixes' `suffixes'
		
		foreach str of local trash_strings {
			di "removing: `str'"
			replace firm1 = subinstr(firm1, "`str'", "", .)
			}    

	*2.2 Replace shells with holdings
		replace firm1 = "GLENCORE_XSTRATA" if firm1 == "ANTAPACCAY" | firm1 == "EMPRESA_MINERA_LOS_QUENUALES"    
		replace firm1 = "GLENCORE_XSTRATA" if firm1 == "XSTRATA_TINTAYA"    
		replace firm1 = "VOLCAN"           if firm1 == "CHUNGAR"
	
	replace firm = subinstr(firm1,"_"," ",.)
	drop firm1
	
*3. Clean "mine" variable (mine name):
    gen mine1=mine
	
	*3.1 Drop prefixes and suffixes [note: the suffixes block could be shortened with some regex lines]
		*Remove whitespace (restored after the block)
		replace mine1=subinstr(mine1," ","_",.)    
		local prefixes  ACUMULACION_ MINAS_DE_COBRE_ PLANTA_
		local suffixes __A) __B) __C) __D) __E) __F) __G) __H) __I) __J)
		local suffixes `suffixes'  _II _I  -1609 -2013 -3A -12 -13 -14 -89 -97 -98 -1 -2 -3 -5 -6 -7 -8 
		local suffixes `suffixes' _N_1 _N_2 _N_6-A098 _N_7-41-A _N_7-41-B _N1 _1,2,3 _1126 _41 _42 _1 _2 _3 
		local trash_strings `prefixes' `suffixes'
		
		foreach str of local trash_strings {
			di "removing: `str'"
			replace mine1 = subinstr(mine1, "`str'", "", .)
			}   	
    *3.2 Corrections
	    *3.2.1 ANDES: fix two mines having the name "ANDES"
		    replace mine1 = "ANDES_-_AREQUIPA" if (mine=="ANDES" | mine=="ANDES 1") & firm != "AUREX"
		    replace mine1 = "ANDES_-_PASCO"    if  mine=="ANDES"                    & firm == "AUREX" 
		*3.2.2 ANTAPACCAY: distinction between ANTAPACCAY's units is ignored (else incompatibility with Catastro dataset)
		    replace mine1 = "ANTAPACCAY" if mine=="PLTA._INDUSTRIAL_DE_OXIDOS" | mine1 == "TINTAYA"
		*3.2.3 AQUILES: this mine got registered with owners' name
		    replace mine1 = "AQUILES" if mine1 == "MILPO"
		*3.2.4 CASAPALCA: Casapalca is the shell-name of Americana's owner, rename for merge with Catastro dataset
		    replace mine1 = "CASAPALCA" if mine=="AMERICANA" 
		*3.2.5 BATEAS: "Bateas" has a different name here and at Catastro, both names must refer to the same mine because their location is identical
		    replace mine1 = "HUAYLLACHO" if firm == "BATEAS" 
		*3.2.6 CPS: as in (3.2.5)
		    replace mine1 = "SAN_NICOLAS" if mine1 == "CPS"
		*3.2.7 SANTA ROSA: fix many mines having the name "SANTA ROSA"
		    replace mine1 = "SANTA_ROSA_others" if mine1 == "SANTA_ROSA" & firm !="SANTA ROSA"
		*3.2.8 TOQUEPALA: as in (3.2.2)
		    replace mine1 = "TOQUEPALA" if mine1 == "TOTORAL"
		*3.2.9 YANACOCHA: as in (3.2.2)
		    replace mine1 = "YANACOCHA" if firm == "YANACOCHA"
	replace mine = subinstr(mine1,"_"," ",.)
	drop mine1
	
sort dep prov dist year value_MM

*4. Edit polygon names for consistency with 'ubigeo' dataset
	replace dist = "NASCA"   if dist == "NAZCA"
	replace prov = "NASCA"   if prov == "NAZCA"
	replace dist = "ESPINAR" if dist == "YAURI"
	replace prov = "OYON"    if dist == "OYON"
save "Mines/Production/out/mines_production_01_17.dta", replace

*5. Districts Panel dataset
    drop if dep == "REGIONAL"
	collapse (sum) value_MM, by(dep prov dist year)
	
	merge m:1 dep prov dist using "Mines/Production/in/ubigeos.dta", keep(1 3) keepusing(dep prov dist ubigeo) nogen

	*4.1 bring in population and calculate pc production value
		merge 1:1 ubigeo year using "Population/population INEI.dta", keepusing(population) nogen
		foreach var in dep prov dist {
		    rename `var' `var'1
			}
		merge m:1 ubigeo using "Mines/Production/in/ubigeos.dta", keep(1 3) keepusing(dep prov dist) nogen
		foreach var in dep prov dist {
		    replace `var' = `var'1 if `var' == ""
			drop `var'1
			}
		replace value_MM = 0 if value_MM == .
		gen value_pc = 1000000*value_MM/population
		format value_pc %15.0fc
		gsort -value_pc
		compress
save "Mines/Production/out/dists_production_01_17.dta", replace	
	
sort year	
local line1  (line value_MM year if ubigeo == "021014")
local line2  (line value_MM year if ubigeo == "040127") 
local line3  (line value_MM year if ubigeo == "030506") 
local line4  (line value_MM year if ubigeo == "120805")
local line5  (line value_MM year if ubigeo == "080801")
local line6  (line value_MM year if ubigeo == "180106")
local line7  (line value_MM year if ubigeo == "230302")
local line8  (line value_MM year if ubigeo == "110203")
local line9  (line value_MM year if ubigeo == "080708")
local line10  (line value_MM year if ubigeo == "060101")

local lab1 lab(1  "San Marcos, Áncash")
local lab1 lab(2  "Yarabamba, Arequipa")
local lab1 lab(3  "Challhuahuacho, Apurímac")
local lab1 lab(4  "Morococha, Junín")
local lab1 lab(5  "Espinar, Cusco")
local lab1 lab(6  "San Marcos, Ancash")
local lab1 lab(7  "San Marcos, Ancash")
local lab1 lab(8  "San Marcos, Ancash")
local lab1 lab(9  "San Marcos, Ancash")
local lab1 lab(10 "San Marcos, Ancash")


local lines   `line1' `line2' `line3' `line4' `line5' `line6' `line7' `line8' `line9' `line10'
local leg_lab `lab1'  `lab2'  `lab3'  `lab4'  `lab5'  `lab6'  `lab7'  `lab8'  `lab9'  `lab10'
local leg legend(`leg_lab' pos(3) cols(1) size(small))
twoway `lines', graphregion(fcolor(white))	`leg' 
	
	
	
*For use in paper		
		*		drop if year<2004 | year>2011
		*replace value_MM_aduanas = 0 if value_MM_aduanas == .
		*replace value_MM_WB      = 0 if value_MM_WB      == .
		*gen     value_pc_aduanas = value_MM_aduanas*1000000/population
		*gen     value_pc_WB      = value_MM_WB*1000000/population
		*preserve
		*	collapse (sum) value_pc_aduanas value_pc_WB, by(ubigeo)
		*	rename value_pc_aduanas value_m_pc_dist
		*	label var value_m_pc_dist "Cumm. Sum of pc mining production (current USD)"
		*	save "00_Data/Mines/Production/out/prod_districts.dta",replace	
		*restore
		*gen prov_code = substr(ubigeo, 1, 4) 
		*collapse (sum) value_MM_aduanas value_MM_WB population, by(prov_code year)
		*gen     value_pc_aduanas = value_MM_aduanas*1000000/population
		*gen     value_pc_WB      = value_MM_WB*1000000/population			
		*collapse value_pc_aduanas value_pc_WB population, by(prov_code)
		*rename value_pc value_m_pc_prov
		*label var value_pc_aduanas "Cumm. Sum of pc mining production (current USD)"
		*save "00_Data/Mines/Production/out/prod_provinces.dta",replace	
		*save "/Users/Sebastian/Documents/Papers/Ketchup/00_Data/out/mn_prod_provinces_fromTIE.dta",replace	
