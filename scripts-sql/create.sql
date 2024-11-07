
create table empresa(
	id serial primary key,
	nome_fantasia varchar(100) not null,
	cnpj varchar(14) not null,
	razao_social text
);

create table funcionario(
	id serial primary key,
	nome varchar(100) not null,
	cpf varchar(11) not null,
	telefone varchar(11) not null,
	ocupacao varchar(100),
	id_empresa integer not null,
	foreign key (id_empresa) references empresa (id)
);

create table medico(
	id serial primary key,
	nome varchar(100) not null,
	crm varchar(100) not null,
	especialidade varchar(200) not null
);

create table exame(
	id serial primary key,
	tipo_exame varchar(100) not null,
	valor_exame decimal(10,2) not null
);

create table exame_funcionario(
	id serial primary key,
	id_exame integer not null,
	id_funcionario integer not null,
	data_exame timestamp not null,
	foreign key (id_funcionario) references funcionario (id),
	foreign key (id_exame) references exame (id)
);

create table consulta(
	id serial primary key,
	id_funcionario integer not null,
	id_medico integer not null,
	data_consulta timestamp not null,
	descricao text not null,
	foreign key (id_funcionario) references funcionario (id),
	foreign key (id_medico) references medico (id)
);

create table emissao_atestado(
	id serial primary key,
	id_consulta integer not null,
	id_medico integer not null,
	cid varchar(7) not null,
	descricao text not null,
	data_emissao timestamp not null,
	foreign key (id_consulta) references consulta (id),
	foreign key (id_medico) references medico (id)
);
select * from empresa;

ALTER TABLE empresa
  ALTER COLUMN cnpj TYPE VARCHAR(20);

  ALTER TABLE funcionario
  ALTER COLUMN cpf TYPE VARCHAR(20);

    ALTER TABLE funcionario
  ALTER COLUMN telefone TYPE VARCHAR(20);


 Select * from funcionario;



