-- Médico com mais atestado

SELECT m.nome, COUNT(ea.id) AS total_atestados
FROM medico m
JOIN emissao_atestado ea ON ea.id_medico = m.id
GROUP BY m.id
ORDER BY total_atestados DESC
LIMIT 1;

-- Tipos de exames realizados

SELECT e.tipo_exame, COUNT(ef.id) AS total_exames
FROM exame e
JOIN exame_funcionario ef ON ef.id_exame = e.id
GROUP BY e.id
ORDER BY total_exames DESC
LIMIT 5;

-- Riscos Ocupacionais mais comuns
SELECT ea.cid, COUNT(ea.id) AS total_riscos
FROM emissao_atestado ea
GROUP BY ea.cid
ORDER BY total_riscos DESC;

-- tempo médio de emissão de atestado
SELECT 
  (SUM(EXTRACT(EPOCH FROM (ea.data_emissao - c.data_consulta)) / 3600) / COUNT(*)) AS tempo_medio_horas
FROM 
  emissao_atestado ea
JOIN 
  consulta c ON c.id = ea.id_consulta
WHERE 
  ea.data_emissao IS NOT NULL
  AND c.data_consulta IS NOT NULL
  AND ea.data_emissao > c.data_consulta;

-- funcionario com mais atestado
 SELECT f.nome, COUNT(ea.id) AS total_atestados
FROM funcionario f
JOIN consulta c ON c.id_funcionario = f.id
JOIN emissao_atestado ea ON ea.id_consulta = c.id
GROUP BY f.id
ORDER BY total_atestados DESC
LIMIT 5;

-- exames por empresa
SELECT 
  f.id_empresa,
  e.nome_fantasia AS nome,
  COUNT(ef.id) AS total_exames
FROM 
  exame_funcionario ef
JOIN 
  funcionario f ON f.id = ef.id_funcionario
JOIN 
  empresa e ON e.id = f.id_empresa
GROUP BY 
  f.id_empresa, e.nome_fantasia
ORDER BY 
  total_exames DESC;
 
-- relação entre ocupação e causa
 SELECT f.ocupacao, e.tipo_exame, COUNT(ef.id) AS total_exames
FROM exame_funcionario ef
JOIN funcionario f ON f.id = ef.id_funcionario
JOIN exame e ON e.id = ef.id_exame
GROUP BY f.ocupacao, e.tipo_exame
ORDER BY total_exames DESC;

-- Taxa de aprovação dos exames
SELECT e.tipo_exame, 
       SUM(CASE WHEN ea.cid LIKE '??%' THEN 1 ELSE 0 END) AS aprovado, 
       SUM(CASE WHEN ea.cid NOT LIKE '??%' THEN 1 ELSE 0 END) AS reprovado
FROM exame_funcionario ef
JOIN exame e ON e.id = ef.id_exame
JOIN emissao_atestado ea ON ea.id_consulta = ef.id
GROUP BY e.tipo_exame;

-- Taxa de reprovação por medico
SELECT m.nome, 
       SUM(CASE WHEN ea.cid NOT LIKE '??%' THEN 1 ELSE 0 END) AS total_reprovados, 
       COUNT(ea.id) AS total_atestados, 
       (SUM(CASE WHEN ea.cid NOT LIKE '??%' THEN 1 ELSE 0 END) * 100.0 / COUNT(ea.id)) AS taxa_reprovacao
FROM medico m
JOIN emissao_atestado ea ON ea.id_medico = m.id
GROUP BY m.id
ORDER BY taxa_reprovacao DESC;

-- Riscos por empresa
SELECT f.ocupacao, ea.cid, COUNT(ea.id) AS total_riscos
FROM emissao_atestado ea
JOIN consulta c ON c.id = ea.id_consulta
JOIN funcionario f ON f.id = c.id_funcionario
GROUP BY f.ocupacao, ea.cid
ORDER BY total_riscos DESC;

-- Variação sazonal

SELECT EXTRACT(MONTH FROM ef.data_exame) AS mes, 
       COUNT(ef.id) AS total_exames
FROM exame_funcionario ef
GROUP BY mes
ORDER BY mes;




