# Databricks notebook source
# MAGIC %run ../../../01_utils/utils

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# Se obtiene la lista de tablas que se procesaran de raw to landing
list_table=conf_json_order()
logger = init_logging("staging")

mount='/mnt/data_s4'
raw='raw'
t_capa ='staging'
v_year=date_process('yyyy')
v_current = date_process('yyyymmddhhmmss')


# recorremos todas las tablas a procesar
for table_landing in list_table :   
    #obtiene valores de tabla
    t_table= table_landing['table']['name'].lower()
    t_partition= table_landing['table']['partition_field'].lower()
    
    t_primary_key =table_landing['primary_key']
    t_location_delta = f'{mount}/{t_capa}/{t_table}'
        
    if t_partition == 'd':
        partition = 'year_month_day'
    else:
        partition = 'year_month'
        
    logger.info(f'Procesando tabla: {t_table} - partición : {t_partition}')
    
    # Omitir columnas para select
    noCols = ['create_at',partition,'origin_file']
    # noCols = ['create_at']
    
    source_landing= f'landing.{t_table}'
    # Obtener columnas para Bronze y Silver
    columns = get_columns_to_select(source_landing)
    
    df_origin = read_df_max_landing(partition,source_landing,columns)
     
    df = df_origin.drop(*noCols)
    df = df.dropDuplicates()
    
    logger.info('latets_landing :' +str(df.count()))

    exis_table = existe_table(f'{t_capa}',t_table)
    #logger.info(f'existe Tabla : {exis_table}')
    
    if exis_table == False :
            create_table(t_location_delta,f'{t_capa}.{t_table}',t_partition,df_origin)
            logger.info(f'creacion de tabla : {t_table}')
    
    else :
        logger.info(f'merge tabla: {t_table}')
        logger.info(f'primary_key: {t_primary_key}')
        
        #######################
        #### condition
        #######################
        condition =''
        primary_key =''
        for field in t_primary_key :
            condition = f'{t_table}.{field} = {t_table}.{field}'
            primary_key += condition+' and '

        primary_key = primary_key[:-4]
        
        #######################
        #### update 
        #######################
        
        lista_columns_update=columns.split(",")

        update_merge=''
        list_update_merge =''
        
        for remove_item in t_primary_key :
            lista_columns_update.remove(remove_item)

        for fields_update in lista_columns_update:
            update_merge= f'{t_table}.{fields_update}= sqlconsult.{fields_update},'
            list_update_merge += update_merge 
    
        list_update_merge = list_update_merge[:-1]
        
        #######################
        #### insert 
        #######################
        
        lista_columns_insert=columns.split(",")
        
        insert_merge=''
        list_insert_merge =''
        
        for fields_insert in lista_columns_insert:
            insert_merge= f'sqlconsult.{fields_insert},'
            list_insert_merge += insert_merge 
        
        list_insert_merge = list_insert_merge[:-1]
        
        #######################
        #### script merge
        #######################
        latest_query = f"""
        merge into {t_capa}.{t_table}
        using ( select {columns} from {source_landing} 
                  where create_at >= (select max(create_at) from {source_landing} )
                  limit 100
                ) sqlconsult on {primary_key}
        when matched then
        update set {list_update_merge}
        
        when not matched then
        insert ({columns})
        values ({list_insert_merge})
        """ 
        #logger.info(latest_query)
        #logger.info(latest_query)
        spark.sql(latest_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select origin_file,create_at,count(1) 
# MAGIC from  landing.kna1 
# MAGIC --where  CREATE_AT >= (select max(CREATE_AT) from landing.kna1 )
# MAGIC group by origin_file,create_at

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC merge into staging.kna1
# MAGIC         using ( select mandt,kunnr,land1,name1,name2,ort01,pstlz,regio,sortl,stras,telf1,telfx,xcpdk,adrnr,mcod1,mcod2,mcod3,anred,aufsd,bahne,bahns,bbbnr,bbsnr,begru,brsch,bubkz,datlt,erdat,ernam,exabl,faksd,fiskn,knazk,knrza,konzs,ktokd,kukla,lifnr,lifsd,locco,loevm,name3,name4,niels,ort02,pfach,pstl2,counc,cityc,rpmkr,sperr,spras,stcd1,stcd2,stkza,stkzu,telbx,telf2,teltx,telx1,lzone,xzemp,vbund,stceg,dear1,dear2,dear3,dear4,dear5,gform,bran1,bran2,bran3,bran4,bran5,ekont,umsat,umjah,uwaer,jmzah,jmjah,katr1,katr2,katr3,katr4,katr5,katr6,katr7,katr8,katr9,katr10,stkzn,umsa1,txjcd,periv,abrvw,inspbydebi,inspatdebi,ktocd,pfort,werks,dtams,dtaws,duefl,hzuor,sperz,etikg,civve,milve,kdkg1,kdkg2,kdkg3,kdkg4,kdkg5,xknza,fityp,stcdt,stcd3,stcd4,stcd5,xicms,xxipi,xsubt,cfopc,txlw1,txlw2,ccc01,ccc02,ccc03,ccc04,bonded_area_confirm,donate_mark,cassd,knurl,j_1kfrepre,j_1kftbus,j_1kftind,confs,updat,uptim,nodel,dear6,cvp_xblck,suframa,rg,exp,uf,rgdate,ric,rne,rnedate,cnae,legalnat,crtn,icmstaxpay,indtyp,tdt,comsize,decregpc,kna1_eew_cust,rule_exclusion,vso_r_palhgt,vso_r_pal_ul,vso_r_pk_mat,vso_r_matpal,vso_r_i_no_lyr,vso_r_one_mat,vso_r_one_sort,vso_r_uld_side,vso_r_load_pref,vso_r_dpoint,alc,pmt_office,fee_schedule,duns,duns4,psofg,psois,pson1,pson2,pson3,psovn,psotl,psohs,psost,psoo1,psoo2,psoo3,psoo4,psoo5,j_1iexcd,j_1iexrn,j_1iexrg,j_1iexdi,j_1iexco,j_1icstno,j_1ilstno,j_1ipanno,j_1iexcicu,aedat,usnam,j_1isern,j_1ipanref,j_3getyp,j_3greftyp,pspnr,coaufnr,j_3gagext,j_3gagint,j_3gagdumi,j_3gagstdi,lgort,kokrs,kostl,j_3gabglg,j_3gabgvg,j_3gabrart,j_3gstdmon,j_3gstdtag,j_3gtagmon,j_3gzugtag,j_3gmaschb,j_3gmeinsa,j_3gkeinsa,j_3gblsper,j_3gkleivo,j_3gcalid,j_3gvmonat,j_3gabrken,j_3glabrech,j_3gaabrech,j_3gzutvhlg,j_3gnegmen,j_3gfristlo,j_3geminbe,j_3gfmgue,j_3gzuschue,j_3gschprs,j_3ginvsta,sapcem_dber,sapcem_kvmeq,create_at,origin_file,year_month_day from landing.kna1 
# MAGIC         where  create_at >= (select max(create_at) from landing.kna1 )
# MAGIC                 ) sqlconsult on kna1.mandt = kna1.mandt and kna1.kunnr = kna1.kunnr and kna1.land1 = kna1.land1 
# MAGIC         when matched then
# MAGIC         update set kna1.name1= sqlconsult.name1,kna1.name2= sqlconsult.name2,kna1.ort01= sqlconsult.ort01,kna1.pstlz= sqlconsult.pstlz,kna1.regio= sqlconsult.regio,kna1.sortl= sqlconsult.sortl,kna1.stras= sqlconsult.stras,kna1.telf1= sqlconsult.telf1,kna1.telfx= sqlconsult.telfx,kna1.xcpdk= sqlconsult.xcpdk,kna1.adrnr= sqlconsult.adrnr,kna1.mcod1= sqlconsult.mcod1,kna1.mcod2= sqlconsult.mcod2,kna1.mcod3= sqlconsult.mcod3,kna1.anred= sqlconsult.anred,kna1.aufsd= sqlconsult.aufsd,kna1.bahne= sqlconsult.bahne,kna1.bahns= sqlconsult.bahns,kna1.bbbnr= sqlconsult.bbbnr,kna1.bbsnr= sqlconsult.bbsnr,kna1.begru= sqlconsult.begru,kna1.brsch= sqlconsult.brsch,kna1.bubkz= sqlconsult.bubkz,kna1.datlt= sqlconsult.datlt,kna1.erdat= sqlconsult.erdat,kna1.ernam= sqlconsult.ernam,kna1.exabl= sqlconsult.exabl,kna1.faksd= sqlconsult.faksd,kna1.fiskn= sqlconsult.fiskn,kna1.knazk= sqlconsult.knazk,kna1.knrza= sqlconsult.knrza,kna1.konzs= sqlconsult.konzs,kna1.ktokd= sqlconsult.ktokd,kna1.kukla= sqlconsult.kukla,kna1.lifnr= sqlconsult.lifnr,kna1.lifsd= sqlconsult.lifsd,kna1.locco= sqlconsult.locco,kna1.loevm= sqlconsult.loevm,kna1.name3= sqlconsult.name3,kna1.name4= sqlconsult.name4,kna1.niels= sqlconsult.niels,kna1.ort02= sqlconsult.ort02,kna1.pfach= sqlconsult.pfach,kna1.pstl2= sqlconsult.pstl2,kna1.counc= sqlconsult.counc,kna1.cityc= sqlconsult.cityc,kna1.rpmkr= sqlconsult.rpmkr,kna1.sperr= sqlconsult.sperr,kna1.spras= sqlconsult.spras,kna1.stcd1= sqlconsult.stcd1,kna1.stcd2= sqlconsult.stcd2,kna1.stkza= sqlconsult.stkza,kna1.stkzu= sqlconsult.stkzu,kna1.telbx= sqlconsult.telbx,kna1.telf2= sqlconsult.telf2,kna1.teltx= sqlconsult.teltx,kna1.telx1= sqlconsult.telx1,kna1.lzone= sqlconsult.lzone,kna1.xzemp= sqlconsult.xzemp,kna1.vbund= sqlconsult.vbund,kna1.stceg= sqlconsult.stceg,kna1.dear1= sqlconsult.dear1,kna1.dear2= sqlconsult.dear2,kna1.dear3= sqlconsult.dear3,kna1.dear4= sqlconsult.dear4,kna1.dear5= sqlconsult.dear5,kna1.gform= sqlconsult.gform,kna1.bran1= sqlconsult.bran1,kna1.bran2= sqlconsult.bran2,kna1.bran3= sqlconsult.bran3,kna1.bran4= sqlconsult.bran4,kna1.bran5= sqlconsult.bran5,kna1.ekont= sqlconsult.ekont,kna1.umsat= sqlconsult.umsat,kna1.umjah= sqlconsult.umjah,kna1.uwaer= sqlconsult.uwaer,kna1.jmzah= sqlconsult.jmzah,kna1.jmjah= sqlconsult.jmjah,kna1.katr1= sqlconsult.katr1,kna1.katr2= sqlconsult.katr2,kna1.katr3= sqlconsult.katr3,kna1.katr4= sqlconsult.katr4,kna1.katr5= sqlconsult.katr5,kna1.katr6= sqlconsult.katr6,kna1.katr7= sqlconsult.katr7,kna1.katr8= sqlconsult.katr8,kna1.katr9= sqlconsult.katr9,kna1.katr10= sqlconsult.katr10,kna1.stkzn= sqlconsult.stkzn,kna1.umsa1= sqlconsult.umsa1,kna1.txjcd= sqlconsult.txjcd,kna1.periv= sqlconsult.periv,kna1.abrvw= sqlconsult.abrvw,kna1.inspbydebi= sqlconsult.inspbydebi,kna1.inspatdebi= sqlconsult.inspatdebi,kna1.ktocd= sqlconsult.ktocd,kna1.pfort= sqlconsult.pfort,kna1.werks= sqlconsult.werks,kna1.dtams= sqlconsult.dtams,kna1.dtaws= sqlconsult.dtaws,kna1.duefl= sqlconsult.duefl,kna1.hzuor= sqlconsult.hzuor,kna1.sperz= sqlconsult.sperz,kna1.etikg= sqlconsult.etikg,kna1.civve= sqlconsult.civve,kna1.milve= sqlconsult.milve,kna1.kdkg1= sqlconsult.kdkg1,kna1.kdkg2= sqlconsult.kdkg2,kna1.kdkg3= sqlconsult.kdkg3,kna1.kdkg4= sqlconsult.kdkg4,kna1.kdkg5= sqlconsult.kdkg5,kna1.xknza= sqlconsult.xknza,kna1.fityp= sqlconsult.fityp,kna1.stcdt= sqlconsult.stcdt,kna1.stcd3= sqlconsult.stcd3,kna1.stcd4= sqlconsult.stcd4,kna1.stcd5= sqlconsult.stcd5,kna1.xicms= sqlconsult.xicms,kna1.xxipi= sqlconsult.xxipi,kna1.xsubt= sqlconsult.xsubt,kna1.cfopc= sqlconsult.cfopc,kna1.txlw1= sqlconsult.txlw1,kna1.txlw2= sqlconsult.txlw2,kna1.ccc01= sqlconsult.ccc01,kna1.ccc02= sqlconsult.ccc02,kna1.ccc03= sqlconsult.ccc03,kna1.ccc04= sqlconsult.ccc04,kna1.bonded_area_confirm= sqlconsult.bonded_area_confirm,kna1.donate_mark= sqlconsult.donate_mark,kna1.cassd= sqlconsult.cassd,kna1.knurl= sqlconsult.knurl,kna1.j_1kfrepre= sqlconsult.j_1kfrepre,kna1.j_1kftbus= sqlconsult.j_1kftbus,kna1.j_1kftind= sqlconsult.j_1kftind,kna1.confs= sqlconsult.confs,kna1.updat= sqlconsult.updat,kna1.uptim= sqlconsult.uptim,kna1.nodel= sqlconsult.nodel,kna1.dear6= sqlconsult.dear6,kna1.cvp_xblck= sqlconsult.cvp_xblck,kna1.suframa= sqlconsult.suframa,kna1.rg= sqlconsult.rg,kna1.exp= sqlconsult.exp,kna1.uf= sqlconsult.uf,kna1.rgdate= sqlconsult.rgdate,kna1.ric= sqlconsult.ric,kna1.rne= sqlconsult.rne,kna1.rnedate= sqlconsult.rnedate,kna1.cnae= sqlconsult.cnae,kna1.legalnat= sqlconsult.legalnat,kna1.crtn= sqlconsult.crtn,kna1.icmstaxpay= sqlconsult.icmstaxpay,kna1.indtyp= sqlconsult.indtyp,kna1.tdt= sqlconsult.tdt,kna1.comsize= sqlconsult.comsize,kna1.decregpc= sqlconsult.decregpc,kna1.kna1_eew_cust= sqlconsult.kna1_eew_cust,kna1.rule_exclusion= sqlconsult.rule_exclusion,kna1.vso_r_palhgt= sqlconsult.vso_r_palhgt,kna1.vso_r_pal_ul= sqlconsult.vso_r_pal_ul,kna1.vso_r_pk_mat= sqlconsult.vso_r_pk_mat,kna1.vso_r_matpal= sqlconsult.vso_r_matpal,kna1.vso_r_i_no_lyr= sqlconsult.vso_r_i_no_lyr,kna1.vso_r_one_mat= sqlconsult.vso_r_one_mat,kna1.vso_r_one_sort= sqlconsult.vso_r_one_sort,kna1.vso_r_uld_side= sqlconsult.vso_r_uld_side,kna1.vso_r_load_pref= sqlconsult.vso_r_load_pref,kna1.vso_r_dpoint= sqlconsult.vso_r_dpoint,kna1.alc= sqlconsult.alc,kna1.pmt_office= sqlconsult.pmt_office,kna1.fee_schedule= sqlconsult.fee_schedule,kna1.duns= sqlconsult.duns,kna1.duns4= sqlconsult.duns4,kna1.psofg= sqlconsult.psofg,kna1.psois= sqlconsult.psois,kna1.pson1= sqlconsult.pson1,kna1.pson2= sqlconsult.pson2,kna1.pson3= sqlconsult.pson3,kna1.psovn= sqlconsult.psovn,kna1.psotl= sqlconsult.psotl,kna1.psohs= sqlconsult.psohs,kna1.psost= sqlconsult.psost,kna1.psoo1= sqlconsult.psoo1,kna1.psoo2= sqlconsult.psoo2,kna1.psoo3= sqlconsult.psoo3,kna1.psoo4= sqlconsult.psoo4,kna1.psoo5= sqlconsult.psoo5,kna1.j_1iexcd= sqlconsult.j_1iexcd,kna1.j_1iexrn= sqlconsult.j_1iexrn,kna1.j_1iexrg= sqlconsult.j_1iexrg,kna1.j_1iexdi= sqlconsult.j_1iexdi,kna1.j_1iexco= sqlconsult.j_1iexco,kna1.j_1icstno= sqlconsult.j_1icstno,kna1.j_1ilstno= sqlconsult.j_1ilstno,kna1.j_1ipanno= sqlconsult.j_1ipanno,kna1.j_1iexcicu= sqlconsult.j_1iexcicu,kna1.aedat= sqlconsult.aedat,kna1.usnam= sqlconsult.usnam,kna1.j_1isern= sqlconsult.j_1isern,kna1.j_1ipanref= sqlconsult.j_1ipanref,kna1.j_3getyp= sqlconsult.j_3getyp,kna1.j_3greftyp= sqlconsult.j_3greftyp,kna1.pspnr= sqlconsult.pspnr,kna1.coaufnr= sqlconsult.coaufnr,kna1.j_3gagext= sqlconsult.j_3gagext,kna1.j_3gagint= sqlconsult.j_3gagint,kna1.j_3gagdumi= sqlconsult.j_3gagdumi,kna1.j_3gagstdi= sqlconsult.j_3gagstdi,kna1.lgort= sqlconsult.lgort,kna1.kokrs= sqlconsult.kokrs,kna1.kostl= sqlconsult.kostl,kna1.j_3gabglg= sqlconsult.j_3gabglg,kna1.j_3gabgvg= sqlconsult.j_3gabgvg,kna1.j_3gabrart= sqlconsult.j_3gabrart,kna1.j_3gstdmon= sqlconsult.j_3gstdmon,kna1.j_3gstdtag= sqlconsult.j_3gstdtag,kna1.j_3gtagmon= sqlconsult.j_3gtagmon,kna1.j_3gzugtag= sqlconsult.j_3gzugtag,kna1.j_3gmaschb= sqlconsult.j_3gmaschb,kna1.j_3gmeinsa= sqlconsult.j_3gmeinsa,kna1.j_3gkeinsa= sqlconsult.j_3gkeinsa,kna1.j_3gblsper= sqlconsult.j_3gblsper,kna1.j_3gkleivo= sqlconsult.j_3gkleivo,kna1.j_3gcalid= sqlconsult.j_3gcalid,kna1.j_3gvmonat= sqlconsult.j_3gvmonat,kna1.j_3gabrken= sqlconsult.j_3gabrken,kna1.j_3glabrech= sqlconsult.j_3glabrech,kna1.j_3gaabrech= sqlconsult.j_3gaabrech,kna1.j_3gzutvhlg= sqlconsult.j_3gzutvhlg,kna1.j_3gnegmen= sqlconsult.j_3gnegmen,kna1.j_3gfristlo= sqlconsult.j_3gfristlo,kna1.j_3geminbe= sqlconsult.j_3geminbe,kna1.j_3gfmgue= sqlconsult.j_3gfmgue,kna1.j_3gzuschue= sqlconsult.j_3gzuschue,kna1.j_3gschprs= sqlconsult.j_3gschprs,kna1.j_3ginvsta= sqlconsult.j_3ginvsta,kna1.sapcem_dber= sqlconsult.sapcem_dber,kna1.sapcem_kvmeq= sqlconsult.sapcem_kvmeq,kna1.create_at= sqlconsult.create_at,kna1.origin_file= sqlconsult.origin_file,kna1.year_month_day= sqlconsult.year_month_day
# MAGIC         
# MAGIC         when not matched then
# MAGIC         insert (mandt,kunnr,land1,name1,name2,ort01,pstlz,regio,sortl,stras,telf1,telfx,xcpdk,adrnr,mcod1,mcod2,mcod3,anred,aufsd,bahne,bahns,bbbnr,bbsnr,begru,brsch,bubkz,datlt,erdat,ernam,exabl,faksd,fiskn,knazk,knrza,konzs,ktokd,kukla,lifnr,lifsd,locco,loevm,name3,name4,niels,ort02,pfach,pstl2,counc,cityc,rpmkr,sperr,spras,stcd1,stcd2,stkza,stkzu,telbx,telf2,teltx,telx1,lzone,xzemp,vbund,stceg,dear1,dear2,dear3,dear4,dear5,gform,bran1,bran2,bran3,bran4,bran5,ekont,umsat,umjah,uwaer,jmzah,jmjah,katr1,katr2,katr3,katr4,katr5,katr6,katr7,katr8,katr9,katr10,stkzn,umsa1,txjcd,periv,abrvw,inspbydebi,inspatdebi,ktocd,pfort,werks,dtams,dtaws,duefl,hzuor,sperz,etikg,civve,milve,kdkg1,kdkg2,kdkg3,kdkg4,kdkg5,xknza,fityp,stcdt,stcd3,stcd4,stcd5,xicms,xxipi,xsubt,cfopc,txlw1,txlw2,ccc01,ccc02,ccc03,ccc04,bonded_area_confirm,donate_mark,cassd,knurl,j_1kfrepre,j_1kftbus,j_1kftind,confs,updat,uptim,nodel,dear6,cvp_xblck,suframa,rg,exp,uf,rgdate,ric,rne,rnedate,cnae,legalnat,crtn,icmstaxpay,indtyp,tdt,comsize,decregpc,kna1_eew_cust,rule_exclusion,vso_r_palhgt,vso_r_pal_ul,vso_r_pk_mat,vso_r_matpal,vso_r_i_no_lyr,vso_r_one_mat,vso_r_one_sort,vso_r_uld_side,vso_r_load_pref,vso_r_dpoint,alc,pmt_office,fee_schedule,duns,duns4,psofg,psois,pson1,pson2,pson3,psovn,psotl,psohs,psost,psoo1,psoo2,psoo3,psoo4,psoo5,j_1iexcd,j_1iexrn,j_1iexrg,j_1iexdi,j_1iexco,j_1icstno,j_1ilstno,j_1ipanno,j_1iexcicu,aedat,usnam,j_1isern,j_1ipanref,j_3getyp,j_3greftyp,pspnr,coaufnr,j_3gagext,j_3gagint,j_3gagdumi,j_3gagstdi,lgort,kokrs,kostl,j_3gabglg,j_3gabgvg,j_3gabrart,j_3gstdmon,j_3gstdtag,j_3gtagmon,j_3gzugtag,j_3gmaschb,j_3gmeinsa,j_3gkeinsa,j_3gblsper,j_3gkleivo,j_3gcalid,j_3gvmonat,j_3gabrken,j_3glabrech,j_3gaabrech,j_3gzutvhlg,j_3gnegmen,j_3gfristlo,j_3geminbe,j_3gfmgue,j_3gzuschue,j_3gschprs,j_3ginvsta,sapcem_dber,sapcem_kvmeq,create_at,origin_file,year_month_day)
# MAGIC         values (sqlconsult.mandt,sqlconsult.kunnr,sqlconsult.land1,sqlconsult.name1,sqlconsult.name2,sqlconsult.ort01,sqlconsult.pstlz,sqlconsult.regio,sqlconsult.sortl,sqlconsult.stras,sqlconsult.telf1,sqlconsult.telfx,sqlconsult.xcpdk,sqlconsult.adrnr,sqlconsult.mcod1,sqlconsult.mcod2,sqlconsult.mcod3,sqlconsult.anred,sqlconsult.aufsd,sqlconsult.bahne,sqlconsult.bahns,sqlconsult.bbbnr,sqlconsult.bbsnr,sqlconsult.begru,sqlconsult.brsch,sqlconsult.bubkz,sqlconsult.datlt,sqlconsult.erdat,sqlconsult.ernam,sqlconsult.exabl,sqlconsult.faksd,sqlconsult.fiskn,sqlconsult.knazk,sqlconsult.knrza,sqlconsult.konzs,sqlconsult.ktokd,sqlconsult.kukla,sqlconsult.lifnr,sqlconsult.lifsd,sqlconsult.locco,sqlconsult.loevm,sqlconsult.name3,sqlconsult.name4,sqlconsult.niels,sqlconsult.ort02,sqlconsult.pfach,sqlconsult.pstl2,sqlconsult.counc,sqlconsult.cityc,sqlconsult.rpmkr,sqlconsult.sperr,sqlconsult.spras,sqlconsult.stcd1,sqlconsult.stcd2,sqlconsult.stkza,sqlconsult.stkzu,sqlconsult.telbx,sqlconsult.telf2,sqlconsult.teltx,sqlconsult.telx1,sqlconsult.lzone,sqlconsult.xzemp,sqlconsult.vbund,sqlconsult.stceg,sqlconsult.dear1,sqlconsult.dear2,sqlconsult.dear3,sqlconsult.dear4,sqlconsult.dear5,sqlconsult.gform,sqlconsult.bran1,sqlconsult.bran2,sqlconsult.bran3,sqlconsult.bran4,sqlconsult.bran5,sqlconsult.ekont,sqlconsult.umsat,sqlconsult.umjah,sqlconsult.uwaer,sqlconsult.jmzah,sqlconsult.jmjah,sqlconsult.katr1,sqlconsult.katr2,sqlconsult.katr3,sqlconsult.katr4,sqlconsult.katr5,sqlconsult.katr6,sqlconsult.katr7,sqlconsult.katr8,sqlconsult.katr9,sqlconsult.katr10,sqlconsult.stkzn,sqlconsult.umsa1,sqlconsult.txjcd,sqlconsult.periv,sqlconsult.abrvw,sqlconsult.inspbydebi,sqlconsult.inspatdebi,sqlconsult.ktocd,sqlconsult.pfort,sqlconsult.werks,sqlconsult.dtams,sqlconsult.dtaws,sqlconsult.duefl,sqlconsult.hzuor,sqlconsult.sperz,sqlconsult.etikg,sqlconsult.civve,sqlconsult.milve,sqlconsult.kdkg1,sqlconsult.kdkg2,sqlconsult.kdkg3,sqlconsult.kdkg4,sqlconsult.kdkg5,sqlconsult.xknza,sqlconsult.fityp,sqlconsult.stcdt,sqlconsult.stcd3,sqlconsult.stcd4,sqlconsult.stcd5,sqlconsult.xicms,sqlconsult.xxipi,sqlconsult.xsubt,sqlconsult.cfopc,sqlconsult.txlw1,sqlconsult.txlw2,sqlconsult.ccc01,sqlconsult.ccc02,sqlconsult.ccc03,sqlconsult.ccc04,sqlconsult.bonded_area_confirm,sqlconsult.donate_mark,sqlconsult.cassd,sqlconsult.knurl,sqlconsult.j_1kfrepre,sqlconsult.j_1kftbus,sqlconsult.j_1kftind,sqlconsult.confs,sqlconsult.updat,sqlconsult.uptim,sqlconsult.nodel,sqlconsult.dear6,sqlconsult.cvp_xblck,sqlconsult.suframa,sqlconsult.rg,sqlconsult.exp,sqlconsult.uf,sqlconsult.rgdate,sqlconsult.ric,sqlconsult.rne,sqlconsult.rnedate,sqlconsult.cnae,sqlconsult.legalnat,sqlconsult.crtn,sqlconsult.icmstaxpay,sqlconsult.indtyp,sqlconsult.tdt,sqlconsult.comsize,sqlconsult.decregpc,sqlconsult.kna1_eew_cust,sqlconsult.rule_exclusion,sqlconsult.vso_r_palhgt,sqlconsult.vso_r_pal_ul,sqlconsult.vso_r_pk_mat,sqlconsult.vso_r_matpal,sqlconsult.vso_r_i_no_lyr,sqlconsult.vso_r_one_mat,sqlconsult.vso_r_one_sort,sqlconsult.vso_r_uld_side,sqlconsult.vso_r_load_pref,sqlconsult.vso_r_dpoint,sqlconsult.alc,sqlconsult.pmt_office,sqlconsult.fee_schedule,sqlconsult.duns,sqlconsult.duns4,sqlconsult.psofg,sqlconsult.psois,sqlconsult.pson1,sqlconsult.pson2,sqlconsult.pson3,sqlconsult.psovn,sqlconsult.psotl,sqlconsult.psohs,sqlconsult.psost,sqlconsult.psoo1,sqlconsult.psoo2,sqlconsult.psoo3,sqlconsult.psoo4,sqlconsult.psoo5,sqlconsult.j_1iexcd,sqlconsult.j_1iexrn,sqlconsult.j_1iexrg,sqlconsult.j_1iexdi,sqlconsult.j_1iexco,sqlconsult.j_1icstno,sqlconsult.j_1ilstno,sqlconsult.j_1ipanno,sqlconsult.j_1iexcicu,sqlconsult.aedat,sqlconsult.usnam,sqlconsult.j_1isern,sqlconsult.j_1ipanref,sqlconsult.j_3getyp,sqlconsult.j_3greftyp,sqlconsult.pspnr,sqlconsult.coaufnr,sqlconsult.j_3gagext,sqlconsult.j_3gagint,sqlconsult.j_3gagdumi,sqlconsult.j_3gagstdi,sqlconsult.lgort,sqlconsult.kokrs,sqlconsult.kostl,sqlconsult.j_3gabglg,sqlconsult.j_3gabgvg,sqlconsult.j_3gabrart,sqlconsult.j_3gstdmon,sqlconsult.j_3gstdtag,sqlconsult.j_3gtagmon,sqlconsult.j_3gzugtag,sqlconsult.j_3gmaschb,sqlconsult.j_3gmeinsa,sqlconsult.j_3gkeinsa,sqlconsult.j_3gblsper,sqlconsult.j_3gkleivo,sqlconsult.j_3gcalid,sqlconsult.j_3gvmonat,sqlconsult.j_3gabrken,sqlconsult.j_3glabrech,sqlconsult.j_3gaabrech,sqlconsult.j_3gzutvhlg,sqlconsult.j_3gnegmen,sqlconsult.j_3gfristlo,sqlconsult.j_3geminbe,sqlconsult.j_3gfmgue,sqlconsult.j_3gzuschue,sqlconsult.j_3gschprs,sqlconsult.j_3ginvsta,sqlconsult.sapcem_dber,sqlconsult.sapcem_kvmeq,sqlconsult.create_at,sqlconsult.origin_file,sqlconsult.year_month_day)

# COMMAND ----------

#variables generales : 
bronze ='bronze'
silver ='silver'
bronze_table_name ='ztotc_costprec'
silver_table_delta = 'ztotc_costprec'
source_bronze= f'{bronze}.{bronze_table_name}'
source_silver= f'{silver}.{silver_table_delta}'
mount='/mnt/data_s4'
raw='raw'
data='data'

# metodo : date_process -> obtiene formato yyyy
v_year=date_process('yyyy')
v_current = date_process('yyyymmddhhmmss')

file_location = f'{mount}/{raw}/{bronze_table_name}/{data}/{v_year}/'
#print(file_location)
# metodo : max_file_storage -> obtiene maximo valor del archivo
max_file=max_file_storage(file_location)
#print(max_file)
name_file = max_file.get("name")

# Omitir columnas para select
noCols = ['create_at','year_month_day','origin_file']
# noCols = ['create_at']

# Obtener columnas para Bronze y Silver
columns = getColumnsToSelect(source_bronze)

#bronze query
partition1 = getPartition2(source_bronze,'year_month_day',-1)
print("partition1 :", partition1)
#bronze query
latest_query = f"""
select {columns}
from {source_bronze} 
where year_month_day = {partition1}
""" 

latets_bronze = spark.sql(latest_query)
latets_bronze = latets_bronze.drop(*noCols)
latets_bronze = latets_bronze.dropDuplicates()
print ('latets_bronze :' +str(latets_bronze.count()))


#bronze query anterior
partition2 = getPartition2(source_bronze,'year_month_day',-2)
print("partition2 :", partition2)
#bronze query
latest_query2 = f"""
select {columns}
from {source_bronze} 
where year_month_day = {partition2}
""" 

latets_bronze2 = spark.sql(latest_query2)
latets_bronze2 = latets_bronze2.drop(*noCols)
latets_bronze2 = latets_bronze2.dropDuplicates()
print ('latets_bronze2 :' +str(latets_bronze2.count()))

#delta
delta=latets_bronze.exceptAll(latets_bronze2)

#metodo : transfor_basic --> tranformacion basica 
delta=transfor_basic(delta,'basic','')

#metodo : transfor_basic --> tipo de dato decimal 
list_decimal=['prec_plan','prec_real','cost_plan','cost_real']
delta=transfor_basic(delta,'decimal',list_decimal)

#metodo : transfor_basic --> tipo de dato integer 
list_integer=['sku']
delta=transfor_basic(delta,'integer',list_integer)


if delta .count() > 0:
    print ('delta :' +str(delta.count()))
    condition = f"year_month_day='{name_file[0:8]}'"
    delta = delta\
    .withColumn('create_at', f.unix_timestamp(f.lit(v_current),'yyyy-MM-dd HH:mm:ss').cast("timestamp")) \
    .withColumn('year_month_day', f.lit(name_file[0:8])) \
    .withColumn('origin_file', f.lit(name_file))  #name_file
    delta.write.mode('overwrite').format('delta').option("replaceWhere", condition).saveAsTable(source_silver)  
    print('execution ok')
