update connector.instance
set allowed_uses = array(select distinct unnest(allowed_uses || array['a2a.cancel']))
where provider = 'A2A_REVIEW_PEER' and not ('a2a.cancel' = any(allowed_uses));
