-- gl_je_headers en dwh.
truncate table gb_mdl_mexico_costoproducir.gl_je_headers;
insert overwrite table gb_mdl_mexico_costoproducir.gl_je_headers partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_gl_je_headers tmp join (select  je_header_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_gl_je_headers group by je_header_id) sec on tmp.je_header_id = sec.je_header_id  and tmp.storeday = sec.first_record;
-- gl_je_lines en dwh.
truncate table gb_mdl_mexico_costoproducir.gl_je_lines;
insert overwrite table gb_mdl_mexico_costoproducir.gl_je_lines partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_gl_je_lines tmp join (select  je_header_id ,je_line_num ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_gl_je_lines group by je_header_id ,je_line_num) sec on tmp.je_header_id = sec.je_header_id and tmp.je_line_num = sec.je_line_num and tmp.storeday = sec.first_record;
-- gl_estructura_contable en dwh.
truncate table gb_mdl_mexico_costoproducir.gl_estructura_contable;
insert overwrite table gb_mdl_mexico_costoproducir.gl_estructura_contable partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_gl_estructura_contable tmp join (select  code_combination_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_gl_estructura_contable group by code_combination_id) sec on tmp.code_combination_id = sec.code_combination_id  and tmp.storeday = sec.first_record;