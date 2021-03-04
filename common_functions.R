

# id lists
pnrs <- c("v_pnr_encrypted", "personnummer_encrypted", "patient_cpr_encrypted", "cpr_encrypted", "v_cpr_encrypted", "k_cprnr_encrypted")
rids <- c("k_recnum", "v_recnum", "kontakt_id_encrypted", "kontakt_id")

drv      <- NULL # censored 
conn_def <- NULL # censored

# pulling data in chunks from slow-joe SQL connection and then building a local database of binary datasets with much easier access
build_db <- function(projct, db_dir, prefix, prefix.add=NULL, mode, ids=NULL, drv, conn_def){
  
  # make sure output dir exists
  dir.create(db_dir, recursive = T)
  
  # logging
  sink(paste0(db_dir, "/00_build_rdb.log"))
  print(paste0("building db, slicing on first ", prefix, " characters"))
  
  # id names
  pnrs <- c("v_pnr_encrypted", "personnummer_encrypted", "patient_cpr_encrypted", "cpr_encrypted", "v_cpr_encrypted", ids$pnrs)
  rids <- c("k_recnum", "v_recnum", "kontakt_id_encrypted", "kontakt_id", ids$rids)
  
  # get list of data sources accessed by project
  con <- dbConnect(drv, conn_def)
  vws <- dbGetQuery(con, paste0("select name from sys.objects where type in('V','U') and schema_id=Schema_ID('FSEID0000", projct, "')"))[,1]
  try(dbDisconnect(con))
  
  # exclude or push to last iterations the biggest chunks not relevant for population definition/matching
  last <- c("LAB_LAB_DM_FORSKER", "LMS_EPIKUR", "DS_LMS_EPIKUR", "LAB_DM_FORSKER", "DS_LAB_DM_FORSKER")
  last <- last[last %in% vws]
  
  frst <- vws[!vws %in% last]
  vws <- c(frst, last[last %in% vws])
  
  aux <- as.character()
  
  for (vw in vws){
    print(paste0(vw, " in progress at ", Sys.time(), "..."))
    
    # open connection, read table, close connection
    con <- dbConnect(drv, conn_def)
    try(tmp <- dbGetQuery(con, paste0("Select top ", 5, " * from FSEID0000", projct, ".", vw)))
    try(dbDisconnect(con))
    
    if (is.null(tmp)){next} # skip if data not read
    
    # clear existing
    if (mode=="clear"){
      try(unlink(paste0(db_dir, "/", vw), recursive = T))
      try(file.remove(paste0(db_dir, "/", vw, ".RDS"), recursive = T))
    }
    
    # identify index columns to use for slicing
    index <- as.character()
    t.names <- tolower(colnames(tmp))
    if (sum(t.names %in% pnrs)>0){
      index <- t.names[t.names %in% pnrs][1]
    } else if (sum(t.names %in% rids)>0){
      index <- t.names[t.names %in% rids][1]
    } 
    
    # read and write in slices to reduce load on sql/odbc
    if (length(na.omit(index))==1){
      
      # find source's indexes
      con <- dbConnect(drv, conn_def)
      try(indxs <- dbGetQuery(con, paste0("Select DISTINCT LEFT(", toupper(index), ", ", prefix, ") from FSEID0000", projct, ".", vw)))
      if (length(unique(na.omit(indxs[,1])))==1 | vw %in% prefix.add) {
        try(indxs <- dbGetQuery(con, paste0("Select DISTINCT LEFT(", toupper(index), ", ", prefix+1, ") from FSEID0000", projct, ".", vw)))
      }
      try(dbDisconnect(con))
      
      # convert index list to vector
      indxs <- indxs[,1][order(indxs[,1])]
      
      # identify, which index subsets are already available
      if (dir.exists(paste0(db_dir, "/", vw))) {
        local_db <- gsub(".RDS", "", list.files(paste0(db_dir, "/", vw)))
        
        # only fill with indexes that are still missing in table
        if (exists("local_db")){if (length(local_db)>0) {indxs <- indxs[!indxs %in% substr(local_db, 1, prefix)]}}
      } else (dir.create(paste0(db_dir, "/", vw)))
      
      for (s in indxs){
        print(paste0("reading subset ", s, " (", which(indxs==s), "/", length(indxs), ") of ", vw, " from remote db at ", Sys.time()))
        
        # connect, read, disconnect
        con <- dbConnect(drv, conn_def)
        try(tmp <- dbGetQuery(con, paste0("Select * from FSEID0000", projct, ".", vw, " WHERE LEFT(", toupper(index), ", ", prefix, ") like '", s, "'")))
        try(dbDisconnect(con))
        
        # skip if data not read
        if (!exists("tmp")){print(paste0(s, " for ", vw, " not saved to local db! refill to complete data transfer.")); next}
        
        # show top of data and write to local db
        print(paste0("writing ", vw, " (", which(vws==vw), "/", length(vws), ") to local db..."))
        colnames(tmp) <- tolower(colnames(tmp))
        
        # write to db
        colnames(tmp)[colnames(tmp) %in% pnrs] <- "pnr_enc"
        colnames(tmp)[colnames(tmp) %in% rids] <- "rid_enc"
        saveRDS(tmp, paste0(db_dir, "/", vw, "/", s, ".RDS"))
        
        # placeholder, wipe and clean
        rm(list=c("tmp", "con"))
        gc(); rJava::.jgc()
      }
    } else {
      
      # store information on tables with no pnrs/rids indices
      aux <- c(aux, vw)
      
      # no index available, pull all data in one chunk
      print(paste0("index var not there, pulling whole table at ", Sys.time()))
      
      # connect, read, disconnect
      con <- dbConnect(drv, conn_def)
      try(tmp <- dbGetQuery(con, paste0("Select * from FSEID0000", projct, ".", vw)))
      try(dbDisconnect(con))
      
      # skip if data not read
      if (is.null(tmp)){print(paste0(vw, " not read")); next}
      
      # show top of data and write to local db
      print(paste0("writing ", vw, " to local db..."))
      colnames(tmp) <- tolower(colnames(tmp))
      
      # write to db
      colnames(tmp)[colnames(tmp) %in% pnrs] <- "pnr_enc"
      colnames(tmp)[colnames(tmp) %in% rids] <- "rid_enc"
      saveRDS(tmp, paste0(db_dir, "/", vw, ".RDS"))
      
      # placeholder, wipe and clean
      rm(list=c("tmp", "con"))
      gc(); rJava::.jgc()
    }
  }
  
  # saving names of db tables with no indexing (definitions, labels, etc)
  saveRDS(aux, paste0(db_dir, "/aux_sources.RDS"))
  
  print("00_build_rdb.R done!")
  sink()
}

# pulling data from local binary db. 
local.pull <- function(path, vw, ids=NULL, vars=NULL, id.type="pnr"){
  
  # print status
  print(paste0("pulling local ", vw, " at ", Sys.time()))
  
  # map sources
  if (file.exists(paste0(path, "/", toupper(vw), ".RDS"))){
    sources <- paste0(path, "/", toupper(vw), ".RDS")
  } else {
    sources <- list.files(paste0(path, "/", vw))
  }
  
  # stop if no data exists as expected
  if (length(sources)==0) {
    stop("data not there")
  }
  
  if (id.type=="pnr"){
    id.ref <- pnrs
  } else if (id.type=="rid"){
    id.ref <- rids
  }
  
  # read column names
  if (length(sources)==1){
    tmp <- readRDS(sources)
    vw.cols <- colnames(tmp)
    
    # identify id columns
    id.col <- vw.cols[vw.cols %in% id.ref]
    if (length(id.col)==0){
      return(tmp)
    } else if (length(id.col)>1){
      stop("multiple possible id cols in source")
    } else {
      colnames(tmp)[colnames(tmp)==id.col] <- paste0(id.type, "_enc")
      if (is.null(vars)){vars <- colnames(tmp)}
      return(tmp[,colnames(tmp)[colnames(tmp) %in% c(paste0(id.type, "_enc"), vars)]])
    }
  } else if (length(sources)>1) {
    
    # collect data and return as one object
    if (is.null(ids)){
      tmp <- list()
      for (s in sources){
        print(paste0("reading ", s, " (", which(sources==s), "/", length(sources), ") at ", Sys.time()))
        tmp[[s]] <- readRDS(paste0(path, "/", vw, "/", s))
        colnames(tmp[[s]])[colnames(tmp[[s]]) %in% id.ref] <- paste0(id.type, "_enc")
        if (is.null(vars)){vars <- colnames(tmp[[s]])}
        tmp[[s]] <- tmp[[s]][,colnames(tmp[[s]])[colnames(tmp[[s]]) %in% c(paste0(id.type, "_enc"), vars)]]
      }
      return(dplyr::bind_rows(tmp))
    } 
    
    # only keep data where ids match input ids
    if (!is.null(ids)){
      
      # split ids
      if (id.type=="rid"){
        end <- ifelse(length(unique(substr(ids, 1, 1)))==1, 2, 1) # split at 2nd character if 1st are identical
      } else {end <- 1}
      ids <- ids[order(ids)]
      ids <- split(as.character(ids), factor(substr(ids, 1, end), levels=unique(substr(ids, 1, end))))

      # collect data and return as one object
      tmp <- list()
      for (s in names(ids)){
        print(paste0("reading ", s, " (", which(names(ids)==s), "/", length(ids), ") at ", Sys.time()))
        try(tmp[[s]] <- readRDS(paste0(path, "/", vw, "/", s, ".RDS")))
        if (is.null(tmp[[s]])){print(paste0("no data exists for id subset ", s, ". skipping.")); next}
        vw.col <- colnames(tmp[[s]])
        id.col <- vw.col[vw.col %in% id.ref]
        colnames(tmp[[s]])[vw.col==id.col] <- paste0(id.type, "_enc")
        if (is.null(vars)){vars <- colnames(tmp[[s]])}
        tmp[[s]] <- tmp[[s]][tmp[[s]][[paste0(id.type, "_enc")]] %in% unlist(ids[[s]]),colnames(tmp[[s]])[colnames(tmp[[s]]) %in% c(paste0(id.type, "_enc"), vars)]]
      }
      return(dplyr::bind_rows(tmp))
    }
  }
}

### Initial processing of raw data
# agglomerates a high dimensional set of hierarchized features into smaller groups. Reduces dimensions and keeps hierarchical structure. 
agglomerate <- function(data, domain, id="pnr_enc", selection=NULL, skip=0, levels, threshold, censor.time=NULL, selection.pos=1, free=F, fixed=NULL){
  d <- data
  
  # Settings & check
  if (is.null(d[["date"]])){stop("missing date column in data")}
  if (is.null(d[[domain]])){stop("missing domain column in data")}
  if (is.null(censor.time)){censor.time <- max(d[["date"]], na.rm=T)}
  level.upper <- levels[length(levels)]
  
  # Formatting according to skip and selection
  if (!is.null(selection)){d <- d[substr(d[[domain]], selection.pos, selection.pos+nchar(selection)-1)==selection,]}
  if (skip>0){d[[domain]] <- substr(d[[domain]], skip+1, nchar(d[[domain]]))}
  
  # Generating hierarchical observations
  for (level in levels){
    d[[paste0(domain, level)]] <- substr(d[[domain]], 1, level)
  }
  
  # Counting number of individuals per diagnosis subgroup
  data.table::setDT(d)
  d[ date<=censor.time & !is.na(get(paste0(domain, level.upper))), paste0("affected", level.upper) := length(unique(get(id))), by=eval(paste0(domain, level.upper))]
  if (length(levels)>1){
    for (i in (length(levels)-1):1){
    level <- levels[i]
    level.above <- levels[i+1]
    if (length(censor.time)==2) {
      d[ date>=censor.time[1] &  date<=censor.time[2] & !is.na(paste0(domain, level)) & get(paste0("affected", level.above)) < threshold, paste0("affected", level) := length(unique(get(id))), by=eval(paste0(domain, level))]
    } else {
      d[ date<=censor.time & !is.na(paste0(domain, level)) & get(paste0("affected", level.above)) < threshold, paste0("affected", level) := length(unique(get(id))), by=eval(paste0(domain, level))]
    }
    }
  }
  
  # Organizing observations in agglomerated categories
  d[ get(paste0("affected", level.upper)) >= threshold, x := get(paste0(domain, level.upper))]
  if (length(levels)>1){
    for (i in (length(levels)-1):1){
      level <- levels[i]
      level.above <- levels[i+1]
      d[ get(paste0("affected", level)) >= threshold & get(paste0("affected", level.above)) < threshold , x := get(paste0(domain, level))]
    }
  } 
  
  if (!is.null(fixed)){
    for (f in fixed){
      d[ substr(get(domain), 1, nchar(f))==f , fix := f]
    }
    
    check <- list(pre.fix=table(d$x, d$fix))
    d$x[!is.na(d$fix)] <- d$fix[!is.na(d$fix)]
    check$post.fix <- table(d$x[!is.na(d$fix)], d$fix[!is.na(d$fix)])
    d$fix <- NULL
  }
  
  if (free==T){
    o <- list()
    o[["d"]] <- as.data.frame(d)[,c(id, "date", "x")]
    o[[domain]] <- sort(table(d$x), decreasing = T)
    if (length(censor.time)==2) {
      o[[paste0(domain, ".free")]] <- unique(d[[id]][d[["date"]]>=censor.time[1] & d[["date"]]<=censor.time[2]])
    } else {
      o[[paste0(domain, ".free")]] <- unique(d[[id]][d[["date"]]<=censor.time])
    }
  } else {
    o[["d"]] <- as.data.frame(d)[,c(id, "date", "x")]
    o[[domain]] <- sort(table(d$x), decreasing = T)
  }
  
  if (!is.null(fixed)){o$fixed.check <- check}
              
  return(o)
}

# count observations from one 'value' variable containing qualitative codes of any kind. Enforces temporal censoring to avoid leaks from test to training periods.
observe <- function(data, id, index.date, value, interval, valid.values, bin=F){
  d <- data[,c(id, index.date, value, "date")]
  
  # subsetting on specified date interval
  d$time <- as.numeric(difftime(d$date, d[[index.date]], units = "days"))
  d[[value]][(d$time<interval[1] | d$time>interval[2])] <- NA
  d$time[d$time<interval[1] | d$time>interval[2]] <- NA
  d <- d[(d$time>=interval[1] & d$time<=interval[2]) | is.na(d$time),]
  
  # ignoring values from invalid codes
  if (!is.null(valid.values)){
    d[[value]][!d[[value]] %in% valid.values] <- NA
    d <- d[!is.na(d[[value]]),]
  }
  
  # counting observations of all relevant values per person in interval
  d$k <- as.numeric(!is.na(d[[value]]))
  data.table::setDT(d)
  d[ , k := sum(k), by=c(id, value)]
  d <- as.data.frame(d)[,c(id, index.date, value, "k")]
  d <- unique(d) ## sorting out two registrations on same date
  
  # if binary indicators should replace counts:
  if (bin==T){d$k <- as.numeric(d$k>=1)}
  
  # reshaping counts of observations to wide (each value has its own column)
  e <- list()
  for (v in unique(d[[value]])){
    print(paste0("building counts for ", v, "..."))
    e[[v]] <- reshape::cast(d[d[[value]]==v,], paste0(id,"~",value), sum, value="k")
  }
  try(e[["NA"]] <- NULL, silent=T)
  e <- purrr::reduce(e, dplyr::full_join, by=id)
  return(e)
}

### count observations from several columns in some interval
observe.wide <- function(data, id, index.date, vars, interval){
  d <- data[,c(id, index.date, vars, "date")]
  
  # subsetting on specified date interval
  d$time <- as.numeric(difftime(d$date, d[[index.date]], units = "days"))
  
  # drop rows with missing timing
  d <- d[(d$time>=interval[1] & d$time<=interval[2]) & !is.na(d$time),]
  
  # blanking out values outside of accepted interval
  for (i in vars){
    print(paste0("removing invalid values for ", i))
    d[[i]][d$time<interval[1] | d$time>interval[2]] <- NA
    #d$time[d$time<interval[1] | d$time>interval[2]] <- NA
  }
  
  # counting observations of all relevant values per person in interval
  data.table::setDT(d)
  for (i in vars){
    print(paste0("counting values for ", i))
    d[ , (i) := sum(get(i)), by=id]
  }
  d <- unique(as.data.frame(d)[,c(id, vars)])
  return(d)
}

### observe the last numeric value observed for any id in any interval
observe.last <- function(data, id, index.date, code, value, interval, valid.codes, text=F){
  d <- data[,c(id, index.date, code, value, "date")]
  
  # subsetting on specified date interval 
  d$time <- as.numeric(difftime(d$date, d[[index.date]], units = "days"))
  
  # drop rows with missing timing or timestamps outside of accepted range
  d <- d[(d$time>=interval[1] & d$time<=interval[2]) & !is.na(d$time),]
  
  # ignoring values from invalid codes
  if (!is.null(valid.codes)){
    d <- d[d[[code]] %in% valid.codes,]
  }
  
  # picking last non-NA
  data.table::setDT(d)
  d <- d[order(get(id), date),]
  d[ !is.na(get(value)), n := 1:.N, by=c(id, code)]
  d[ !is.na(get(value)), N :=   .N, by=c(id, code)]
  
  ## ...by wiping non-last values
  d[ n!=N , (value) := NA ]
  d <- as.data.frame(d)[,c(id, index.date, code, value)]
  
  # reshaping all last observations to wide (each code has its own column)
  e <- list()
  for (c in unique(d[[code]])){
    print(paste0("building counts for ", c, "..."))
    if (text==T){
      e[[c]] <- reshape::cast(d[d[[code]]==c & !is.na(d[[value]]),], paste0(id,"~",code), function(x) names(sort(table(x), T))[1], value=value)
    } else {
      e[[c]] <- reshape::cast(d[d[[code]]==c & !is.na(d[[value]]),], paste0(id,"~",code), function(x) mean(as.numeric(x), na.rm=T), value=value)
    }
  }
  try(e[["NA"]] <- NULL, silent=T)
  return(purrr::reduce(e, dplyr::full_join, by=id))
}

### observe date of incidence for any value (qualitative code)
observe.incid <- function(data, id, index.date, value, valid.values){
  d <- data[,c(id, index.date, value, "date")]
  
  # ignoring values from invalid codes
  if (!is.null(valid.values)){
    d <- d[d[[value]] %in% valid.values,]
  }
  
  # picking rows with date equal to incid date
  data.table::setDT(d)
  d[, incid := min(date, na.rm=T), by=c("pnr_enc", value) ]
  d <- d[date==incid]
  
  # reshaping all last observations to wide (each code has its own column)
  d <- unique(d)
  e <- reshape::cast(d, paste0(id,"~", value), mean)
  e <- dplyr::mutate_if(e, is.ok.date, lubridate::as_date)
  try(e[["NA"]] <- NULL, silent=T)
  return(e)
}

### generate table
twoway.chi <- function(data, x, group, bin=F, cens, force.two=F, show.na=F) {
  # get data
  d <- data[!is.na(data[[group]]),]
  
  if (force.two==T){
    d[[x]] <- as.numeric(d[[x]]>=1)
  }
  
  if (is.null(cens)){cens <- -Inf}
  
  # generate table, p.value and labels 
  tab <- table(d[[x]], d[[group]], useNA="ifany")

  groups <- colnames(tab)
  if (nrow(tab)<2){stop("too few levels")}
  p <- form.it(chisq.test(tab)$p.value, 3)
  levels <- rownames(tab)
  
  # handle missings explicitly
  if (show.na==T){
    if (is.na(levels[length(levels)]) & sum(is.na(levels))==1) {levels[length(levels)] <- "N/A"}
  }
  
  # combine n and %
  tab <- matrix(rbind(tab, form.it(prop.table(tab, 2)*100, 1)), nrow=nrow(tab))

  if (sum(as.numeric(tab[,1])<=cens)>=1 | sum(as.numeric(tab[,3])<=cens)>=1){
    p <- paste0("n<", cens)     
  }
  
  # add p value
  if (nrow(tab)>=2) {filling <- c(rep(NA, nrow(tab)-1), p)} else {filling <- p}
  if (nrow(tab)>=2 & bin==F){varname <- c(x, rep(NA, nrow(tab)-1))} else {varname <- rep(x, nrow(tab))}
  
  for (i in 1:nrow(tab)){
    tab[i,2][as.numeric(tab[i,1])<=cens] <- paste0("n<", cens) 
    tab[i,4][as.numeric(tab[i,3])<=cens] <- paste0("n<", cens) 
    tab[i,1][as.numeric(tab[i,1])<=cens] <- paste0("n<", cens) 
    tab[i,3][as.numeric(tab[i,3])<=cens] <- paste0("n<", cens) 
  }
  
  # wrap up, force regular colnames
  tab  <- as.data.frame(cbind(varname, levels, tab, filling))
  colnames(tab) <- c("var", "level", rbind(paste0(groups, ".mean/n"), paste0(groups, ".sd/%")), "p.val")
  
  # pick last row if bin
  if (bin==T){tab <- tab[nrow(tab),]}
  return(tab)
}

# count observations from simple 'n' columns (first row in classic table 1)
twoway.n <- function(data, x, group){
  d <- data[!is.na(data[[group]]),]
  tab <- table(d[[x]], d[[group]], useNA="ifany")
  groups <- colnames(tab)
  if (nrow(tab)>1){stop("too many levels for n-count")}
  tab <- c(rbind(tab, form.it(prop.table(tab, 2)*100, 1)))
  tab <- c(x, NA, tab, NA)
  tab <- as.data.frame(t(tab))
  colnames(tab) <- c("var", "level", rbind(paste0(groups, ".mean/n"), paste0(groups, ".sd/%")), "p.val")
  return(tab)
}

# compares groups in numeric variables. adds
twoway.num <- function(data, x, group, digit.m=1, digit.sd=1, inf=FALSE, cal.date=F){
  d <- data[!is.na(data[[group]]),]
  groups <- unique(na.omit(d[[group]]))
  tab <- rep(NA, 2*length(groups)+2)
  tab[1] <- x
  tab[2] <- NA
  k <- 3
  for (i in 1:length(groups)){
    if (cal.date==F){tab[k+0] <- form.it(mean(d[[x]][d[[group]]==groups[i]], na.rm=T), digit.m)
    } else {
      tab[k+0] <- as.character(lubridate::as_date(round(mean(d[[x]][d[[group]]==groups[i]], na.rm=T))))
    }
    tab[k+1] <- form.it(  sd(d[[x]][d[[group]]==groups[i]], na.rm=T), digit.sd)
    k <- k+2
  }

  # inferential test (rank or t-based)
  if (length(groups)>2){
    tab[length(tab)+1] <- tryCatch({form.it(pnorm(abs(coef(summary(MASS::polr(paste0("as.factor(", x, ")~as.factor(", group, ")"), data=d)))[1,3]), lower.tail = F)*2, 3)}, error=function(err) NA)
  } else {
    tab[length(tab)+1] <- form.it(wilcox.test(as.formula(paste0("as.numeric(", x, ")~as.factor(", group, ")")), data=na.omit(d[,c(x, group)]))$p.value, 3)
  }
  tab <- as.data.frame(t(tab))
  colnames(tab) <- c("var", "level", rbind(paste0(groups, ".mean/n"), paste0(groups, ".sd/%")), "p.val")
  return(tab)
}

# compiling tables from different data types in one table.
tabulate <- function(data, xs, treat, num=NA, cat=NA, bin=NA, dichotomize=NA, cal.date=NA, cens=5, show.na=F){
  t <- data.frame()
  for (x in xs){
    print(paste0("working on ", x))
    if (x %in% "n"){try(t <- dplyr::bind_rows(t, twoway.n  (data=data, x, treat)))}
    if (x %in% num){try(t <- dplyr::bind_rows(t, twoway.num(data=data, x, treat, digit.m = 2, digit.sd = 2)))}
    if (x %in% cal.date){try(t <- dplyr::bind_rows(t, twoway.num(data=data, x, treat, digit.m = 2, digit.sd = 2, cal.date==T)))}
    if (x %in% cat){try(t <- dplyr::bind_rows(t, twoway.chi(data=data, x, treat, cens=cens, show.na=show.na)))}
    if (x %in% dichotomize & !x %in% bin){try(t <- dplyr::bind_rows(t, twoway.chi(data=data, x, treat, cens=cens, force.two=T, show.na=show.na)))}
    if (x %in% bin &         !x %in% dichotomize){try(t <- dplyr::bind_rows(t, twoway.chi(data=data, x, treat, cens=cens, bin=T, show.na=show.na)))}
    if (x %in% bin &          x %in% dichotomize){try(t <- dplyr::bind_rows(t, twoway.chi(data=data, x, treat, cens=cens, force.two=T, bin=T, show.na=show.na)))}
  }
  t <- t%>% dplyr::mutate_if(is.ok, function(x) as.numeric(as.character(x)))
  t <- t%>% dplyr::mutate_if(is.factor, function(x) as.character(x))
  return(t)
}

