import psycopg2
from concurrent.futures import ThreadPoolExecutor
import random
import time
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('pt_BR')

def create_connection():
    return psycopg2.connect(
        host="localhost",
        database="medicina-trabalho",
        user="postgres",
        password="root"
    )

def insert_data(table, data):
    conn = create_connection()
    cursor = conn.cursor()

    try:
        if table == 'empresa':
            query = "INSERT INTO empresa (nome_fantasia, cnpj, razao_social) VALUES (%s, %s, %s)"
        elif table == 'funcionario':
            query = "INSERT INTO funcionario (nome, cpf, telefone, ocupacao, id_empresa) VALUES (%s, %s, %s, %s, %s)"
        elif table == 'medico':
            query = "INSERT INTO medico (nome, crm, especialidade) VALUES (%s, %s, %s)"
        elif table == 'exame':
            query = "INSERT INTO exame (tipo_exame, valor_exame) VALUES (%s, %s)"
        elif table == 'exame_funcionario':
            query = "INSERT INTO exame_funcionario (id_exame, id_funcionario, data_exame) VALUES (%s, %s, %s)"
        elif table == 'consulta':
            query = "INSERT INTO consulta (id_funcionario, id_medico, data_consulta, descricao) VALUES (%s, %s, %s, %s)"
        elif table == 'emissao_atestado':
            query = "INSERT INTO emissao_atestado (id_consulta, id_medico, cid, descricao, data_emissao) VALUES (%s, %s, %s, %s, %s)"
        else:
            raise ValueError(f"Tabela {table} não reconhecida.")

        cursor.executemany(query, data)
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table}: {e}")
    finally:
        cursor.close()
        conn.close()

# Geração de dados
def generate_empresa_data(num_rows):
    data = []
    suffixes = ["Consultoria", "Tecnologia", "Soluções", "Serviços", "Empreendimentos", "Logística", "Financeira", "Indústria", "Comércio"]
    prefixes = ["Global", "Prime", "Grupo", "Master", "Nacional", "Integral", "Premium"]
    
    for _ in range(num_rows):
        prefix = random.choice(prefixes)  
        base_name = fake.company() 
        suffix = random.choice(suffixes)  

        nome_fantasia = f"{prefix} {base_name} {suffix}"
        cnpj = fake.cnpj()
        razao_social = fake.company_suffix()
        data.append((nome_fantasia, cnpj, razao_social))
    return data

def generate_funcionario_data(num_rows, empresa_ids):
    data = []
    for _ in range(num_rows):
        nome = fake.name()
        cpf = fake.cpf()
        telefone = fake.phone_number()
        ocupacao = fake.job()
        id_empresa = random.choice(empresa_ids)
        data.append((nome, cpf, telefone, ocupacao, id_empresa))
    return data

def generate_medico_data(num_rows):
    data = []
    especialidades = [
        'Cardiologia', 'Pediatria', 'Dermatologia', 'Psiquiatria', 'Ortopedia', 'Neurologia', 
        'Ginecologia', 'Oftalmologia', 'Endocrinologia', 'Oncologia', 'Anestesiologia', 
        'Urologia', 'Geriatria', 'Infectologia', 'Reumatologia', 'Cirurgia Geral', 
        'Cirurgia Plástica', 'Cirurgia Vascular', 'Nefrologia', 'Medicina de Família', 
        'Medicina Intensiva', 'Otorrinolaringologia', 'Medicina do Trabalho', 'Pneumologia', 
        'Hematologia', 'Radiologia', 'Genética Médica', 'Nutrição', 'Fisioterapia', 'Psicologia', 
        'Odontologia', 'Mastologia', 'Patologia', 'Medicina Nuclear', 'Cardiologia Pediátrica', 
        'Dermatologia Pediátrica', 'Pneumologia Pediátrica', 'Neurocirurgia', 'Osteopatia', 
        'Fonoaudiologia', 'Homeopatia', 'Acupuntura', 'Terapia Intensiva Neonatal', 'Psiquiatria Infantil', 
        'Medicina Esportiva', 'Medicina Legal', 'Medicina Preventiva', 'Alergologia', 'Imunologia', 
        'Toxicologia', 'Medicina Veterinária', 'Medicina Estética', 'Nefrologia Pediátrica', 
        'Médico do Sono', 'Médico Intensivista', 'Médico de Emergência', 'Neuropsicologia', 
        'Medicina Aeroespacial', 'Medicina de Urgência', 'Medicina Fetal'
    ]
    ufs_brasil = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", 
        "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", 
        "RS", "RO", "RR", "SC", "SP", "SE", "TO"
    ]

    for _ in range(num_rows):
        uf = random.choice(ufs_brasil)
        digits = f"{random.randint(0, 999999):06}"  
   
        nome = fake.name()
        crm = f"{uf}-{digits}"
        especialidade = random.choice(especialidades)
        data.append((nome, crm, especialidade))
    return data

def generate_exame_data(num_rows):
    data = []
    exames = [
        'Hemograma Completo', 'Raio-X', 'Tomografia', 'Eletrocardiograma', 'Ultrassom', 
        'Ressonância Magnética', 'Eletroencefalograma', 'Ultrassonografia Obstétrica', 'Mammografia', 
        'Exame de Urina', 'Exame de Fezes', 'Testes de Função Hepática', 'Testes de Função Renal', 
        'Prova de Função Pulmonar', 'Colooscopia', 'Endoscopia', 'Exame de Colonoscopia', 
        'Exame de Sangue Oculto nas Fezes', 'Teste de Glicemia', 'Teste de Tolerância à Glicose', 
        'Teste de função tireoidiana', 'Teste de gravidez', 'Exame de audiometria', 
        'Teste de Papanicolau', 'Biópsia', 'Exame de Função Cardíaca', 'Densitometria óssea', 
        'Testes de Alergia', 'Exame Oftalmológico', 'Exame de Visão', 'Exame de Acuidade Visual', 
        'Exame de Pressão Arterial', 'Raios-X de Tórax', 'Ultrassonografia Abdominal', 
        'Ultrassonografia de Doppler', 'Exame de Pulso Oximetria', 'Teste de Função Pulmonar', 
        'Ecocardiograma', 'Teste de Esforço', 'Cintilografia', 'Tomografia Computadorizada de Tórax', 
        'Exame de Fezes para Candidíase', 'Exame de Parasitologia', 'Teste de HIV', 'Teste de Hepatite', 
        'Teste de Sífilis', 'Exame de PSA (Antígeno Prostático Específico)', 'Exame de Cálcio Sérico', 
        'Exame de Creatinina', 'Exame de Bilirrubina', 'Teste de Coagulação', 'Exame de Púlsio', 
        'Exame de Fibrinogênio', 'Teste de HIV Rapido', 'Teste de Hepatite C', 'Testes de Função Pulmonar', 
        'Doppler Colorido', 'Exame de Função Visceral', 'Angiografia', 'Espermograma', 'Exame de Função Hepática', 
        'Teste de H. pylori', 'Teste de Estresse Cardíaco', 'Teste de Função Renal', 'Perfil Lipídico', 
        'Exame de PCR (Proteína C Reativa)', 'Exame de Hemoglobina Glicada', 'Testes de Tuberculose', 
        'Teste de Glicose Pós-Prandial', 'Exame de Coração', 'Exame de Pectose'
    ]


    for _ in range(num_rows):
        tipo_exame = random.choice(exames)
        valor_exame = round(random.uniform(50, 300), 2)
        data.append((tipo_exame, valor_exame))
    return data

def generate_exame_funcionario_data(num_rows, exame_ids, funcionario_ids):
    data = []
    for _ in range(num_rows):
        id_exame = random.choice(exame_ids)
        id_funcionario = random.choice(funcionario_ids)
        data_exame = fake.date_this_year(before_today=True, after_today=False)
        data.append((id_exame, id_funcionario, data_exame))
    return data

def generate_consulta_data(num_rows, funcionario_ids, medico_ids):
    data = []
    for _ in range(num_rows):
        id_funcionario = random.choice(funcionario_ids)
        id_medico = random.choice(medico_ids)
        data_consulta = fake.date_this_year(before_today=True, after_today=False)
        descricao = fake.text(max_nb_chars=200)
        data.append((id_funcionario, id_medico, data_consulta, descricao))
    return data

def generate_emissao_atestado_data(num_rows, consulta_ids):
    data = []
    cids = [
    "A00", "A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09",
    "A15", "A16", "A17", "A18", "A19", "A20", "A21", "A22", "A23", "A24",
    "A30", "A31", "A32", "A33", "A34", "A35", "A36", "A37", "A38", "A39",
    "A40", "A41", "A42", "A43", "A44", "A45", "A46", "A47", "A48", "A49",
    "A50", "A51", "A52", "A53", "A54", "A55", "A56", "A57", "A58", "A59",
    "A60", "A61", "A62", "A63", "A64", "A65", "A66", "A67", "A68", "A69",
    "A70", "A71", "A72", "A73", "A74", "A75", "A76", "A77", "A78", "A79",
    "A80", "A81", "A82", "A83", "A84", "A85", "A86", "A87", "A88", "A89",
    "A90", "A91", "A92", "A93", "A94", "A95", "A96", "A97", "A98", "A99",
    "B00", "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09",
    "B15", "B16", "B17", "B18", "B19", "B20", "B21", "B22", "B23", "B24",
    "B25", "B26", "B27", "B28", "B29", "B30", "B31", "B32", "B33", "B34",
    "B35", "B36", "B37", "B38", "B39", "B40", "B41", "B42", "B43", "B44",
    "B50", "B51", "B52", "B53", "B54", "B55", "B56", "B57", "B58", "B59",
    "B60", "B61", "B62", "B63", "B64", "B65", "B66", "B67", "B68", "B69",
    "C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09",
    "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19",
    "C20", "C21", "C22", "C23", "C24", "C25", "C26", "C27", "C28", "C29",
    "C30", "C31", "C32", "C33", "C34", "C35", "C36", "C37", "C38", "C39",
    "C40", "C41", "C42", "C43", "C44", "C45", "C46", "C47", "C48", "C49",
    "D00", "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09",
    "D10", "D11", "D12", "D13", "D14", "D15", "D16", "D17", "D18", "D19",
    "D20", "D21", "D22", "D23", "D24", "D25", "D26", "D27", "D28", "D29",
    "E00", "E01", "E02", "E03", "E04", "E05", "E06", "E07", "E08", "E09",
    "E10", "E11", "E12", "E13", "E14", "E15", "E16", "E17", "E18", "E19",
    "E20", "E21", "E22", "E23", "E24", "E25", "E26", "E27", "E28", "E29",
    "E30", "E31", "E32", "E33", "E34", "E35", "E36", "E37", "E38", "E39",
    "F00", "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "F12", "F13", "F14", "F15", "F16", "F17", "F18", "F19",
    "G00", "G01", "G02", "G03", "G04", "G05", "G06", "G07", "G08", "G09",
    "G10", "G11", "G12", "G13", "G14", "G15", "G16", "G17", "G18", "G19",
    "G20", "G21", "G22", "G23", "G24", "G25", "G26", "G27", "G28", "G29",
    "G30", "G31", "G32", "G33", "G34", "G35", "G36", "G37", "G38", "G39",
    "H00", "H01", "H02", "H03", "H04", "H05", "H06", "H07", "H08", "H09",
    "H10", "H11", "H12", "H13", "H14", "H15", "H16", "H17", "H18", "H19",
    "H20", "H21", "H22", "H23", "H24", "H25", "H26", "H27", "H28", "H29",
    "I00", "I01", "I02", "I03", "I04", "I05", "I06", "I07", "I08", "I09",
    "I10", "I11", "I12", "I13", "I14", "I15", "I16", "I17", "I18", "I19",
    "I20", "I21", "I22", "I23", "I24", "I25", "I26", "I27", "I28", "I29",
    "J00", "J01", "J02", "J03", "J04", "J05", "J06", "J07", "J08", "J09",
    "J10", "J11", "J12", "J13", "J14", "J15", "J16", "J17", "J18", "J19",
    "K00", "K01", "K02", "K03", "K04", "K05", "K06", "K07", "K08", "K09",
    "K10", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18", "K19",
    "K20", "K21", "K22", "K23", "K24", "K25", "K26", "K27", "K28", "K29",
    "L00", "L01", "L02", "L03", "L04", "L05", "L06", "L07", "L08", "L09",
    "L10", "L11", "L12", "L13", "L14", "L15", "L16", "L17", "L18", "L19",
    "L20", "L21", "L22", "L23", "L24", "L25", "L26", "L27", "L28", "L29"
    ]

    for _ in range(num_rows):
        id_consulta, id_medico = random.choice(consulta_ids)
        cid = random.choice(cids)
        descricao = fake.text(max_nb_chars=200)
        data_emissao = fake.date_this_year(before_today=True, after_today=False)
        data.append((id_consulta, id_medico, cid, descricao, data_emissao))
    return data

def insert_in_parallel(num_threads=4, num_rows_per_thread=1000):
    start_time = time.time()

    # Inserir dados de empresas
    empresa_data = generate_empresa_data(num_rows_per_thread)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(insert_data, 'empresa', empresa_data)]
        for future in futures:
            future.result()

    # Recuperar IDs para referência cruzada
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM empresa")
    empresa_ids = [row[0] for row in cursor.fetchall()]

    funcionario_data = generate_funcionario_data(num_rows_per_thread, empresa_ids)
    medico_data = generate_medico_data(num_rows_per_thread)
    exame_data = generate_exame_data(num_rows_per_thread)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(insert_data, 'funcionario', funcionario_data),
            executor.submit(insert_data, 'medico', medico_data),
            executor.submit(insert_data, 'exame', exame_data)
        ]
        for future in futures:
            future.result()

    # Recuperar IDs para próximas inserções
    cursor.execute("SELECT id FROM funcionario")
    funcionario_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT id FROM medico")
    medico_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT id FROM exame")
    exame_ids = [row[0] for row in cursor.fetchall()]

    # Inserir dados em exame_funcionario e consulta
    exame_funcionario_data = generate_exame_funcionario_data(num_rows_per_thread, exame_ids, funcionario_ids)
    consulta_data = generate_consulta_data(num_rows_per_thread, funcionario_ids, medico_ids)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(insert_data, 'exame_funcionario', exame_funcionario_data),
            executor.submit(insert_data, 'consulta', consulta_data)
        ]
        for future in futures:
            future.result()

    # Inserir dados em emissao_atestado
    cursor.execute("SELECT id, id_medico FROM consulta")
    consulta_ids = [(row[0], row[1]) for row in cursor.fetchall()]
    atestado_data = generate_emissao_atestado_data(num_rows_per_thread, consulta_ids)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(insert_data, 'emissao_atestado', atestado_data)]
        for future in futures:
            future.result()

    print(f"Tempo total: {time.time() - start_time} segundos")
    cursor.close()
    conn.close()

insert_in_parallel(num_threads=4, num_rows_per_thread=1000)
