create or replace
procedure         u_30_SRC_DAILY as

begin

--sourcing for issues

scpomgr.u_8d_sourcing;

--create one sourcing record for each exclusive TPM SKU 

insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)

select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
    ' ' abbr, 'ISS1EXCL' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
    0 convenientadjdownpct
from sourcing c, sku ss, sku sd, 

            (select distinct g.item, g.loc dest, g.exclusive_loc source
            from udt_gidlimits_na g, loc l
            where g.exclusive_loc = l.loc
            and l.loc_type = 4
            and g.exclusive_loc is not null
            and g.de = 'E'
            
            union
            
            select distinct g.item, g.loc, g.mandatory_loc 
            from udt_gidlimits_na g, loc l
            where g.mandatory_loc = l.loc
            and l.loc_type = 2
            and g.mandatory_loc is not null
            ) u
    
where u.item = ss.item
and u.source = ss.loc
and u.item = sd.item
and u.dest = sd.loc
and u.item = c.item(+)
and u.dest = c.dest(+)
and c.item is null;

commit;

--Find all possible sources within loc.u_max_dist & loc.u_max_srcs where udt_cost_transit matches source_pc and dest_pc ; 8k

insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)

select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
    ' ' abbr, 'ISS2MAXDISTSRC' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
    0 convenientadjdownpct
from sourcing c, 

    (select u.item, u.dest, u.dest_pc, u.source, u.source_pc, u.u_max_dist, u.u_max_src, u.distance, row_number()
                            over (partition by u.item, u.dest order by cost_pallet, source asc) as rank
    from  

    (select c.item, c.dest, c.dest_pc, c.source, c.source_pc, c.u_max_dist, c.u_max_src, pc.distance,nvl(pc.cost_pallet, 999) cost_pallet
        from
                    
            (select distinct lpad(source_pc, 5, 0) source_pc, lpad(dest_pc, 5, 0) dest_pc, source_co, max(distance) distance, max(cost_pallet) cost_pallet 
            from udt_cost_transit  
            group by lpad(source_pc, 5, 0), lpad(dest_pc, 5, 0), source_co, dest_co
            )  pc, 
                        
            (select f.item, f.loc dest, f.u_max_dist, f.u_max_src, f.dest_pc, p.loc source, p.source_pc
            from

                    (select distinct k.item, i.u_materialcode matcode, k.loc, l.u_max_dist, l.u_max_src, lpad(l.postalcode, 5, 0) dest_pc
                    from skuconstraint k, loc l, item i
                    where k.category = 1
                    and k.loc = l.loc
                    and l.loc_type = 3 
                    and k.item = i.item
                    and i.u_stock = 'C'
                    and k.qty > 0
                    and not exists ( select '1' from udt_gidlimits_na gl 
                                      where gl.loc  = k.loc 
                                        and gl.item = k.item 
                                        and gl.mandatory_loc is not null )  
                    ) f,

                    (select distinct p.outputitem item, p.loc, lpad(l.postalcode, 5, 0) source_pc
                    from productionyield p, item i, loc l
                    where p.outputitem = i.item
                    and i.u_stock = 'C' 
                    and p.loc = l.loc
                    and l.loc_type = 2
                    ) p,
                            
                    (select distinct v.dmdunit item, v.loc, max(v.u_dfu_grp) u_dfu_grp
                    from dfuview v, loc l
                    where v.loc = l.loc
                    and l.loc_type = 3
                    and v.dmdgroup in ('ISS', 'CPU') 
                    group by v.dmdunit, v.loc
                    ) v

            where f.item = v.item
            and f.loc = v.loc
            and f.item = p.item 
            ) c
                    
        where c.dest_pc = pc.dest_pc
        and c.source_pc = pc.source_pc 
        
        ) u
        
   where u.distance < u.u_max_dist
   
    ) u
    
where u.rank < u.u_max_src
and u.item = c.item(+)
and u.dest = c.dest(+)
and u.source = c.source(+)
and not exists ( select '1' 
                   from udt_gidlimits_na gl1 
                  where gl1.loc  = u.dest
                    and gl1.item = u.item 
                    and gl1.forbidden_loc = u.source )  
and c.item is null;

commit;

--where no sourcing find closest loc_type = 2 location  ; less than 4k

insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)

select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
    ' ' abbr, 'ISS3CLOSEST' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
    0 convenientadjdownpct
    
from sourcing c, 

    (select u.item, u.dest, u.dest_pc, u.source, u.source_pc, u.u_max_dist, u.u_max_src, u.distance, u.cost_pallet, row_number()
                            over (partition by u.item, u.dest order by cost_pallet, source asc) as rank
    from  

    (select c.item, c.dest, c.dest_pc, c.source, c.source_pc, c.u_max_dist, c.u_max_src, pc.distance,nvl(pc.cost_pallet, 999) cost_pallet
        from
                    
            (select distinct lpad(source_pc, 5, 0) source_pc, lpad(dest_pc, 5, 0) dest_pc, source_co, max(distance) distance, max(cost_pallet) cost_pallet 
            from udt_cost_transit  
            group by lpad(source_pc, 5, 0), lpad(dest_pc, 5, 0), source_co, dest_co
            )  pc, 
                        
            (select f.item, f.loc dest, f.u_max_dist, f.u_max_src, f.dest_pc, p.loc source, p.source_pc
            from

                    (select distinct k.item, i.u_materialcode matcode, k.loc, l.u_max_dist, l.u_max_src, lpad(l.postalcode, 5, 0) dest_pc
                    from skuconstraint k, loc l, item i
                    where k.category = 1
                    and k.loc = l.loc
                    and l.loc_type = 3 
                    and k.item = i.item
                    and i.u_stock = 'C'
                    and k.qty > 0
                    ) f,

                    (select distinct p.outputitem item, p.loc, lpad(l.postalcode, 5, 0) source_pc
                    from productionyield p, item i, loc l
                    where p.outputitem = i.item
                    and i.u_stock = 'C' 
                    and p.loc = l.loc
                    and l.loc_type = 2
                    ) p,
                            
                    (select distinct v.dmdunit item, v.loc, max(v.u_dfu_grp) u_dfu_grp
                    from dfuview v, loc l
                    where v.loc = l.loc
                    and l.loc_type = 3
                    and v.dmdgroup in ('ISS', 'CPU') 
                    group by v.dmdunit, v.loc
                    ) v

            where f.item = v.item
            and f.loc = v.loc
            and f.item = p.item 
            ) c
                    
        where c.dest_pc = pc.dest_pc(+)
        and c.source_pc = pc.source_pc(+) 
        
        ) u
        
   --where u.distance < u.u_max_dist
   
    ) u
    
where u.rank = 1
and u.item = c.item(+)
and u.dest = c.dest(+)
and c.item is null;

commit;

--collections

--Find all possible sources within loc.u_max_dist & loc.u_max_srcs where udt_cost_transit matches source_pc and dest_pc or source_geo and dest_geo; 16k

insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)

select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
    ' ' abbr, 'COLL0FIXED' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
    0 convenientadjdownpct
from sourcing c, 

    (select f.item, f.loc source, f.u_max_dist, f.u_max_src, f.source_pc, c.dest, c.dest_pc
    from 

            (select distinct k.item, k.loc, l.u_max_dist, l.u_max_src, lpad(l.postalcode, 5, 0) source_pc
            from skuconstraint k, loc l, item i
            where k.category = 10
            and k.loc = l.loc
            and l.loc_type = 3
            and k.item = i.item
            and i.u_stock = 'A'
            and k.qty > 0
            ) f,
                        
            (select s.item, c.loc source, c.plant dest, lpad(l.postalcode, 5, 0) dest_pc
            from tmp_coll_na c, sku s, loc l
            where c.plant = s.loc
            and c.plant = l.loc
            ) c
                        
        where f.item = c.item
        and f.loc = c.source
                        
    ) u
    

where u.item = c.item(+)
and u.dest = c.dest(+)
and c.item is null;

commit;

--where unmatched try to find single lowest cost freight

--insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
--    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
--    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
--    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)
--
--select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
--    ' ' abbr, 'COLL1UNMATCHED' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
--    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
--    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
--    0 convenientadjdownpct
--from 
--
--    (select u.item, u.dest, u.dest_pc, u.source, u.source_pc, u.u_max_dist, u.u_max_src, u.distance, u.cost_pallet, row_number()
--                            over (partition by u.item, u.dest order by cost_pallet, source asc) as rank
--    from 
--
--    (select c.item, c.dest, c.dest_pc, c.source, c.source_pc, c.u_max_dist, c.u_max_src, pc.distance,nvl(pc.cost_pallet, 999) cost_pallet
--        from
--                    
--            (select distinct source_pc, dest_pc, source_co, max(distance) distance, max(cost_pallet) cost_pallet 
--            from udt_cost_transit  
--            group by source_pc, dest_pc, source_co, dest_co
--            )  pc, 
--            
--            (select distinct c.item, c.loc source, c.u_max_dist, c.u_max_src, c.source_pc, s.loc dest, s.dest_pc
--             from
--                                     
--                    (select distinct k.item, k.loc, l.u_max_dist, l.u_max_src, l.postalcode source_pc
--                    from skuconstraint k, loc l, item i, sourcing c
--                    where k.category = 10
--                    and k.loc = l.loc
--                    and l.loc_type = 3
--                    and k.item = i.item
--                    and i.u_stock = 'A'
--                    and k.qty > 0
--                    and k.item = c.item(+)
--                    and k.loc = c.source(+)
--                    and c.item is null
--                    ) c,
--                    
--                    (select s.item, s.loc, l.postalcode dest_pc
--                    from sku s, loc l, item i
--                    where s.loc = l.loc
--                    and l.loc_type = 2
--                    and s.item = i.item
--                    and i.u_stock = 'A'
--                    and s.item = i.item
--                    ) s
--                
--                where c.item = s.item
--                ) c
--                    
--        where c.dest_pc = pc.dest_pc(+)
--        and c.source_pc = pc.source_pc(+)
--
--        ) u
--        
--    --where u.distance < u.u_max_dist 
--    
--    ) u
--    
--
--where u.rank = 1;
--
--commit;

--if still unmatched use the zip code to default plant table

insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)

select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
    ' ' abbr, 'COLL3ZIPCODE' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
    0 convenientadjdownpct
from 

    (SELECT k.item, k.loc source, k.postalcode, z.loc dest, k.qty
         FROM sourcing c, tmp_na_zip z, sku s,
         
              (  SELECT DISTINCT k.item, k.loc, lpad(l.postalcode, 5, 0) postalcode, SUM (qty) qty
                   FROM skuconstraint k, item i, loc l
                  WHERE     k.category = 10
                        AND k.item = i.item
                        AND i.u_stock = 'A'
                        AND k.loc = l.loc
                        AND l.loc_type = 3
               GROUP BY k.item, k.loc, l.postalcode
                 HAVING SUM (qty) > 0
                 ) k
                 
        WHERE k.item = c.item(+) 
        AND k.loc = c.source(+)
        and k.postalcode = lpad(z.postalcode, 5, 0)
        and k.item = s.item
        and z.loc = s.loc 
        AND c.item IS NULL
    ) u;

commit;

--TPM relocations (modified 06/19/2015)

insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)
    
with 

allowed_sources ( source,  postal_code, item, res)
 as
  ( select l.loc source,   lpad(l.postalcode, 5, 0) postalcode, i.item, pc.res
      from scpomgr.loc l, scpomgr.udt_active_sites pc, item i, sku y
     where l.loc_type in ('2','4')
       and l.u_area='NA'
       and l.loc=pc.loc
       and (pc.res like '%ARSOURCE' or pc.res like '%RUSOURCE')
       and pc.status=1
       and i.u_stock in ('B','C')
       and y.item=i.item
       and y.loc=l.loc --and y.loc = 'USBO'
       and ( ( substr(i.item,1, instr(i.item,'AR') +1) 
                = substr(pc.res,1, instr(pc.res,'AR') +1)
            )
              or
             ( substr(i.item,1, instr(i.item,'RU') +1) 
                = substr(pc.res,1, instr(pc.res,'RU') +1)
             )
           )
   ),
   
allowed_dests ( dest,  max_dist, postal_code, max_src, item, res)
 as  
  ( select l.loc, l.u_max_dist, lpad(l.postalcode, 5, 0) postalcode, l.u_max_src, i.item, pc.res
      from scpomgr.loc l, scpomgr.udt_active_sites pc, scpomgr.item i, scpomgr.sku sku
     where l.loc_type in ('2','4')
       and l.u_area='NA'
       and l.loc=pc.loc
       and (pc.res like '%ARDEST' or pc.res like '%RUDEST')
       and pc.status=1     
       and i.u_stock in ('B','C')
       and l.loc=sku.loc
       and i.item=sku.item
       and ( ( substr(i.item,1, instr(i.item,'AR') +1) 
                = substr(pc.res,1, instr(pc.res,'AR') +1)
              )
              or
              ( substr(i.item,1, instr(i.item,'RU') +1) 
                = substr(pc.res,1, instr(pc.res,'RU') +1)
              )
           )
  ),
  
lanes (source, source_pc, dest, dest_pc, item, max_dist, max_src, distance, pallet_cost)
   as   
   (  select distinct src.source, lpad(ct.source_pc, 5, 0) source_pc, dest.dest, lpad(ct.dest_pc, 5, 0) dest_pc, src.item, dest.max_dist,dest.max_src, max(ct.distance), max(ct.cost_pallet) 
       from udt_cost_transit ct, allowed_sources src, allowed_dests dest
       where src.source <> dest.dest
         and src.item=dest.item
         and src.postal_code  = ct.source_pc
         and dest.postal_code = ct.dest_pc
         and ( ( substr(src.res,1, instr(src.res,'AR') +1) 
                  = substr(dest.res,1, instr(dest.res,'AR') +1)
              )
              or
              ( substr(src.res,1, instr(src.res,'RU') +1) 
                = substr(dest.res,1, instr(dest.res,'RU') +1)
              )
           )
           having max(ct.distance) < 800 --dest.max_dist
       group by src.source, source_pc, dest.dest, dest_pc, source_co, dest_co, src.item, dest.max_dist, dest.max_src
   ),
   
ranked_lanes ( source, dest, item, max_src, rank)
  as 
   ( select lane.source, lane.dest, lane.item, lane.max_src ,row_number() 
            over (partition by lane.item, lane.dest order by lane.pallet_cost asc) as rank
      from lanes lane
   )  
   
   select distinct rl.item, rl.dest, rl.source, 'TRUCK' transmode, v_init_eff_date eff, 1 factor, ' ' arrivcal, 0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
    ' ' abbr, 'TPM_RELOC' sourcing, v_init_eff_date disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
    100 costpercentage,     0 supplytransfercost,  v_init_eff_date nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
    0 convenientadjdownpct
   from ranked_lanes rl
   where rl.rank <= rl.max_src
   order by rl.dest, rl.item;
      
commit;

--insert into sourcing (item, dest, source, transmode, eff,     factor, arrivcal,     majorshipqty,     minorshipqty,     enabledyndepsw,     shrinkagefactor,     maxshipqty,     abbr,     sourcing,     disc,     
--    maxleadtime,     minleadtime,     priority,     enablesw,     yieldfactor,     supplyleadtime,     costpercentage,     supplytransfercost,     nonewsupplydate,     shipcal,     
--    ff_trigger_control,     pullforwarddur,     splitqty,     loaddur,     unloaddur,     reviewcal,     uselookaheadsw,     convenientshipqty,     convenientadjuppct,     convenientoverridethreshold,     
--    roundingfactor,     ordergroup,     ordergroupmember,     lotsizesenabledsw,     convenientadjdownpct)
--
--select distinct u.item, u.dest, u.source, 'TRUCK' transmode, TO_DATE('01/01/1970','MM/DD/YYYY') eff,     1 factor,    ' ' arrivcal,     0 majorshipqty,     0 minorshipqty,     1 enabledyndepsw,     0 shrinkagefactor,     0 maxshipqty,     
--    ' ' abbr, 'TPM_RELOC' sourcing,     TO_DATE('01/01/1970','MM/DD/YYYY') disc,     1440 * 365 * 100 maxleadtime,     0 minleadtime,     1 priority,     1 enablesw,     100 yieldfactor,     0 supplyleadtime,     
--    100 costpercentage,     0 supplytransfercost,     TO_DATE('01/01/1970','MM/DD/YYYY') nonewsupplydate,     ' ' shipcal,    ''  ff_trigger_control,     0 pullforwarddur,     0 splitqty,     0 loaddur,     0 unloaddur,     
--    ' ' reviewcal,     1 uselookaheadsw,     0 convenientshipqty,     0 convenientadjuppct,     0 convenientoverridethreshold,     0 roundingfactor,     ' ' ordergroup,     ' ' ordergroupmember,     0 lotsizesenabledsw,     
--    0 convenientadjdownpct
--from 
--
--(select t.item, t.matcode, t.source, p.dest
--from 
--
--    (select s.item, s.loc source, r.dest, r.matcode
--    from sku s, item i, udt_tpm_relocation_na r
--    where s.loc = r.source 
--    and s.item = i.item
--    and i.u_materialcode = r.matcode
--    ) t,
--
--    (
--    select s.item, s.loc dest, r.matcode, r.source
--    from udt_tpm_relocation_na r, loc l,
--
--        (select distinct s.item, s.loc, i.u_materialcode
--        from sku s, item i, productionmethod p
--        where s.item = i.item
--        and s.item = p.item
--        and s.loc = p.loc
--        ) s
--
--    where r.dest = l.loc
--    and l.loc_type in (2, 4)
--    and r.dest = s.loc
--    and r.matcode = s.u_materialcode
--    ) p
--
--where t.item = p.item
--and t.source = p.source
--and t.dest = p.dest
--) u;
--
--commit;

declare
  cursor cur_selected is
    select c.item, c.dest, c.source, c.sourcing, t.transittime,
    case when t.transittime < 1 then 0 else round(t.transittime, 0)*1440 end transittime_new
    from sourcing c, u_42_src_costs t
    where c.item = t.item
    and c.dest = t.dest
    and c.source = t.source 
for update of c.minleadtime;

begin
  for cur_record in cur_selected loop
  
    update sourcing
    set minleadtime = cur_record.transittime_new
    where current of cur_selected;
    
  end loop;
  commit;
end;

insert into sourcingdraw (sourcing, eff, item, dest, source, drawqty, qtyuom)

select c.sourcing, to_date('01/01/1970', 'MM/DD/YYYY') eff, c.item, c.dest, c.source, 1 drawqty, 18 qtyuom 
from sourcing c, sourcingdraw d
where c.item = d.item(+)
and c.dest = d.dest(+)
and c.source = d.source(+)
and c.sourcing = d.sourcing(+)
and d.item is null;

commit;

insert into sourcingyield (sourcing, eff, item, dest, source, yieldqty, qtyuom)

select c.sourcing, to_date('01/01/1970', 'MM/DD/YYYY') eff, c.item, c.dest, c.source, 1 yieldqty, 18 qtyuom 
from sourcing c, sourcingyield d
where c.item = d.item(+)
and c.dest = d.dest(+)
and c.source = d.source(+)
and c.sourcing = d.sourcing(+)
and d.item is null;

commit;

insert into res (loc, type,     res,    cal,  cost,     descr,  avgskuchg,   avgfamilychg,  avgskuchgcost,  avgfamilychgcost,     levelloadsw,     
    levelseqnum,  criticalitem, checkmaxcap,  unitpenalty,  adjfactor,  source,  enablesw,  subtype,   qtyuom,   currencyuom,     productionfamilychgoveropt)

select distinct u.dest loc, 5 type,     u.res,     ' '  cal,     0 cost,     ' '  descr,     0 avgskuchg,     0 avgfamilychg,     0 avgskuchgcost,     0 avgfamilychgcost,     0 levelloadsw,     
    1 levelseqnum,     ' '  criticalitem,     1 checkmaxcap,     0 unitpenalty,     1 adjfactor,  u.source,     1 enablesw,     6 subtype,     18 qtyuom,     11 currencyuom,     0 productionfamilychgoveropt
from res r,

    (select distinct c.source, c.dest , c.source||'->'||c.dest res from sourcing c
    ) u

where u.res = r.res(+)
and r.res is null;

commit;

insert into sourcingrequirement (stepnum,     nextsteptiming,     rate,     leadtime,     offset,     enablesw,     sourcing,     eff,     res,     item,     dest,     source,     qtyuom)

select 1 stepnum,     3 nextsteptiming,     1 rate,     0 leadtime,     0 offset,     1 enablesw,     u.sourcing,     to_date('01/01/1970', 'MM/DD/YYYY') eff,     u.res,     u.item,     u.dest,     u.source,     18 qtyuom
from sourcingrequirement r, 

    (select c.item, c.dest, c.source, c.sourcing, c.source||'->'||c.dest res from sourcing c
    ) u
    
where u.item = r.item(+)
and u.dest = r.dest(+)
and u.source = r.source(+)
and u.sourcing = r.sourcing(+)
and r.item is null;

commit;

insert into cost (cost,  enablesw,   cumulativesw,  groupedsw,  sharedsw,  qtyuom,  currencyuom,   accumcal,  maxqty,     maxutilization)

select distinct 'LOCAL:RES:'||u.res||'-202' cost,     1 enablesw,     0 cumulativesw,     0 groupedsw,     0 sharedsw,     18 qtyuom,     11 currencyuom,    ' '   accumcal,     0 maxqty,     0 maxutilization
from cost c, 

    (select c.item, c.dest, c.source, c.sourcing, c.source||'->'||c.dest res, 'LOCAL:RES:'||c.source||'->'||c.dest||'-202' cost  from sourcing c
    ) u
    
where u.cost = c.cost(+)
and c.cost is null;

commit;

insert into costtier (breakqty, category, value, eff, cost)

select distinct 0 breakqty, 303 category, u.value, to_date('01/01/1970', 'MM/DD/YYYY') eff, u.cost
from costtier t, cost e, 

    (select distinct c.source, c.dest , 'LOCAL:RES:'||c.source||'->'||c.dest||'-202' cost, max(nvl(round(t.cost_pallet/480, 3), 10)) value
    from udt_cost_transit t, 
    
        (select distinct c.source, c.dest, ls.postalcode source_pc, ld.postalcode dest_pc  
        from sourcing c, loc ls, loc ld 
        where c.source = ls.loc
        and c.dest = ld.loc
        ) c
        
    where c.source_pc = t.source_pc(+)
    and c.dest_pc = t.dest_pc(+)
    group by c.source, c.dest , 'LOCAL:RES:'||c.source||'->'||c.dest||'-202' 
    ) u
    
where e.cost = u.cost
and u.cost = t.cost(+)
and t.cost is null;

commit;

insert into rescost (category, res, localcost, tieredcost)

select distinct 202 category, u.res, u.cost localcost, ' ' tieredcost
from rescost r, costtier t, 

    (select distinct c.dest, c.source, c.source||'->'||c.dest res, 'LOCAL:RES:'||c.source||'->'||c.dest||'-202' cost  from sourcing c
    ) u
    
where u.cost = t.cost
and u.cost = r.localcost(+)
and r.localcost is null;

commit;

end;
