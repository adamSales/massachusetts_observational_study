cptMatch <-
function (Z, T,blocks, leaveout = 0, class.methods = "forest", metric = "probability",
    ensemble.metric = "mean.prob", perm.N = 1000,
    leaveout.N = 100, comb.methods = c(class.methods, "ensemble"),
    comb.method = "fisher")
{
    T = as.factor(T)
    train.methods = cpt:::gettrainmethods(class.methods)
    test.methods = cpt:::gettestmethods(class.methods)
    if (is.character(metric))
        metric = cpt:::getmetric(metric)
    if (is.character(ensemble.metric))
        ensemble.metric = cpt:::getensemble.metric(ensemble.metric)
    if (is.character(comb.method))
        comb.method = cpt:::getcombmethod(comb.method)
    if ((leaveout > 0) && (leaveout < 1))
        leaveout = ceiling(min(table(T)) * leaveout)
    teststat = rep(NA, length(class.methods))
    nulldist = matrix(NA, perm.N, length(class.methods) + 1)
    colnames(nulldist) = c(class.methods, "ensemble")
    for (method.i in 1:length(class.methods)) {
        teststat = cpt:::getteststat(Z, T, leaveout, train.methods,
            test.methods, metric, ensemble.metric, leaveout.N)
    }
    if (!missing(blocks)){
      stopifnot(length(blocks)==length(T))
      blocks=as.factor(blocks)
      ss=split(seq_along(T),blocks)

      for(i in 1:perm.N){
        newT=T[unsplit(sapply(ss,sample,simplify=FALSE),blocks)]
        nulldist[i, ] = cpt:::getteststat(Z, newT, leaveout, train.methods,
            test.methods, metric, ensemble.metric, leaveout.N)
      }
    }
    else {
        for (i in 1:perm.N) {
            T = T[sample(length(T))]
            nulldist[i, ] = cpt:::getteststat(Z, T, leaveout, train.methods,
                test.methods, metric, ensemble.metric, leaveout.N)
        }
    }
    pvals = rep(NA, ncol(nulldist))
    names(pvals) = colnames(nulldist)
    nullpvaldist = matrix(NA, perm.N, ncol(nulldist))
    colnames(nullpvaldist) = names(pvals)
    for (method.i in 1:ncol(nulldist)) {
        pvals[method.i] = sum(nulldist[, method.i] >= teststat[method.i])/perm.N
        nullpvaldist[, method.i] = 1 - (rank(nulldist[, method.i],
            ties.method = "min") - 1)/perm.N
    }
    nullcombpvaldist = apply(nullpvaldist[, comb.methods, drop = FALSE],
        1, comb.method)
    pval = sum(nullcombpvaldist <= comb.method(pvals[comb.methods]))/perm.N
    if (length(class.methods) == 1)
        return(list(pval = pvals[1], teststat = teststat[1],
            nulldist = nulldist[, 1], pvals = pvals[1]))
    return(list(pval = pval, teststat = teststat, nulldist = nulldist,
        pvals = pvals))
}
