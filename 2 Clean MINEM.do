*************************************************************************
* Peruvian Mining Production dataset - Part 2
* Sebastian Sardon
* Last updated: Feb 6, 2018
* Creates mining production dataset
* Reference period: 2001-2017
*************************************************************************

cap restore
clear all 
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
        *FIX UNASSIGNED: MDDs  output to MADRE DE DIOS, MANU, MADRE DE DIOS (almost 100% of MDD dep's production, and all unassigned is from MDD)
		*                unassigned output from other departments is dropped (300MM in 2016 and 2017, most of it from Puno [!] )
    drop if dep == "REGIONAL" & firm != "MADRE DE DIOS"
	
	replace dist = "MADRE DE DIOS" if dep == "REGIONAL" | dist == "-------"
    replace prov = "MANU"          if dep == "REGIONAL" | prov == "-------"
    replace dep  = "MADRE DE DIOS" if dep == "REGIONAL"

save "Mines/Production/out/mines_production_01_17.dta", replace

*5. Districts Panel dataset
    *should drop 0 obs, else check
	drop if dep == "REGIONAL"
	collapse (sum) value_MM, by(dep prov dist year)
	preserve
	    clear
		gen year = .
		forvalues yy = 2001/2017{
		    append using "Mines/Production/in/ubigeos.dta"
		    replace year = `yy' if year == .
		    }
		tempfile temp
		save `temp', replace
	restore	
	merge m:1 year dep prov dist using `temp', keepusing(dep prov dist ubigeo) nogen

	*4.1 bring in population and calculate pc production value
		merge 1:1 ubigeo year using "Population/population INEI.dta", keep(1 3) keepusing(population) nogen
		foreach var in dep prov dist {
		    rename `var' `var'1
			}
		merge m:1 ubigeo using "Mines/Production/in/ubigeos.dta", keepusing(dep prov dist) nogen
		foreach var in dep prov dist {
		    replace `var' = `var'1 if `var' == ""
			drop `var'1
			}
		gen value_m_pc = 1000000*value_MM/population
		replace value_MM   = 0 if value_MM   == .
		replace value_m_pc = 0 if value_m_pc == .
        format population     %15.0fc
		format value_m_pc %15.0fc
		gsort -value_m_pc
		compress
		
save "Mines/Production/out/dists_production_01_17.dta", replace	


*6. Provinces Panel dataset
		drop value_m_pc
		gen prov_code = substr(ubigeo,1,4)
		collapse (sum) value_MM population, by(prov_code dep prov year)	
		gen value_m_pc = 1000000*value_MM/population
        replace value_m_pc = 0 if value_m_pc == .
		format value_m_pc %15.0fc
		gsort -value_m_pc
		compress
save "Mines/Production/out/provs_production_01_17.dta", replace	
