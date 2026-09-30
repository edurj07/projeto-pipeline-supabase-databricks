# 📗 Tutorial: Como Postar Seu Projeto no GitHub de Forma Profissional

Este tutorial guia você passo a passo para publicar o projeto **Projeto Vendas — Plataforma de Dados E-commerce Brasileiro** no GitHub com qualidade de portfólio.

---

## ✅ Pré-requisitos

- Conta no GitHub criada
- Git instalado na sua máquina (`git --version`)
- Databricks CLI configurado com acesso ao workspace
- Editor de código (VS Code recomendado)

---

## Passo 1: Clonar o Repositório do Databricks

O Databricks permite sincronizar arquivos do workspace para sua máquina local usando a CLI:

```bash
# Criar uma pasta local para o projeto
mkdir projeto-vendas-ecommerce
cd projeto-vendas-ecommerce

# Inicializar o repositório git
git init

# Baixar os arquivos do workspace do Databricks
databricks workspace export /Users/edurj07@gmail.com/transformations --recursive
databricks workspace export /Users/edurj07@gmail.com/testes --recursive
databricks workspace export /Users/edurj07@gmail.com/atualizar_dashboards_nb
databricks workspace export /Users/edurj07@gmail.com/CLAUDE.md
databricks workspace export /Users/edurj07@gmail.com/README.md
```

Organize os arquivos baixados na estrutura do projeto conforme o README.md.

---

## Passo 2: Criar o Repositório no GitHub

1. Acesse [github.com/new](https://github.com/new)
2. **Repository name:** `projeto-vendas-ecommerce`
3. **Description:** `Plataforma de dados completa para e-commerce brasileiro no Databricks Lakehouse — arquitetura medallion, pipeline SDP, dashboards AI/BI e automação de refresh`
4. **Visibility:** Public (para portfólio)
5. **NÃO** marque "Add a README" (já temos um)
6. **NÃO** adicione .gitignore (vamos criar um personalizado)
7. Clique em **Create repository**

---

## Passo 3: Criar o `.gitignore`

Crie um arquivo `.gitignore` na raiz do projeto:

```gitignore
# Databricks
.databricks/
*.dbc
*.whl

# Python
__pycache__/
*.py[cod]
*.egg-info/
.eggs/
dist/
build/

# Ambientes virtuais
venv/
.venv/
env/

# IDEs
.vscode/
.idea/
*.swp
*.swo

# Sistema operacional
.DS_Store
Thumbs.db

# Credenciais e secrets
*.env
*.key
*.pem
secrets.json

# Logs
*.log
logs/
```

---

## Passo 4: Criar uma Licença

Crie um arquivo `LICENSE` (recomendado MIT para portfólio):

```
MIT License

Copyright (c) 2026 Eduardo R. J.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## Passo 5: Primeiro Commit e Push

```bash
# Adicionar todos os arquivos
git add .

# Verificar o que será commitado
git status

# Fazer o primeiro commit
git commit -m "feat: plataforma de dados e-commerce com arquitetura medallion no Databricks Lakehouse

- Arquitetura medallion: bronze (ingestão) → silver (transformação Python) → gold (agregação SQL)
- 5 tabelas gold: vendas_temporais, vendas_produtos, vendas_detalhadas, clientes_segmentacao, precos_competitividade
- Pipeline SDP serverless com materialized views
- 3 dashboards AI/BI publicados (Comercial, Pricing, Customer Success)
- Job orquestrado: pipeline → testes → refresh automático de dashboards
- Notebook de refresh via Databricks SDK (invalidação de cache + republicação)
- Testes automatizados de qualidade de dados
- Dados de: 13/12/2025 a 11/01/2026 | R$ 1.482.116,03 em receita"

# Adicionar o repositório remoto (substitua pelo seu usuário)
git remote add origin https://github.com/SEU-USUARIO/projeto-vendas-ecommerce.git

# Fazer o push
git branch -M main
git push -u origin main
```

---

## Passo 6: Adicionar Tópicos (Tags) ao Repositório

No GitHub, vá em Settings → General → Topics e adicione:

```
databricks  lakehouse  medallion-architecture  data-engineering  etl-pipeline  
spark  sql  python  data-quality  business-intelligence  dashboards  
unity-catalog  supabase  e-commerce  brazilian-portuguese
```

---

## Passo 7: Adicionar Capturas de Tela (Screenshots)

Crie uma pasta `docs/screenshots/` e adicione imagens dos:
- 3 dashboards publicados (Comercial, Pricing, Customer Success)
- Diagrama de arquitetura
- Pipeline SDP no Databricks
- Job com as 3 tasks

Referencie as imagens no README.md:

```markdown
## 📸 Capturas de Tela

### Dashboard Comercial
![Dashboard Comercial](docs/screenshots/dashboard-comercial.png)

### Dashboard Pricing
![Dashboard Pricing](docs/screenshots/dashboard-pricing.png)

### Arquitetura do Pipeline
![Pipeline SDP](docs/screenshots/pipeline-sdp.png)
```

---

## Passo 8: Boas Práticas para Commits Futuros

Use [Conventional Commits](https://www.conventionalcommits.org/):

```bash
# feat: nova funcionalidade
git commit -m "feat: adiciona filtro global por categoria no dashboard Pricing"

# fix: correção de bug
git commit -m "fix: corrige tipo INT para ROW_NUMBER em vendas_produtos"

# docs: documentação
git commit -m "docs: atualiza números de referência no README"

# test: testes
git commit -m "test: adiciona validação de segmentação de clientes"

# refactor: refatoração
git commit -m "refactor: simplifica query de precos_competitividade"
```

---

## Passo 9: Criar um GitHub Profile README (Opcional, mas Recomendado)

Crie um repositório com o mesmo nome do seu usuário do GitHub e adicione um README.md:

```markdown
# Olá! 👋 Sou Eduardo

## Data Engineer | Databricks | Python | SQL

### 🔥 Projeto em Destaque
- [Projeto Vendas — E-commerce Brasileiro](https://github.com/SEU-USUARIO/projeto-vendas-ecommerce)
  - Plataforma de dados completa no Databricks Lakehouse
  - Arquitetura medallion, pipeline SDP, 3 dashboards AI/BI
  - Automação de refresh de dashboards via SDK
```

---

## ✅ Checklist Final

- [ ] README.md profissional e completo
- [ ] .gitignore configurado
- [ ] LICENSE adicionada
- [ ] Tópicos/tags adicionados no GitHub
- [ ] Screenshots dos dashboards e arquitetura
- [ ] Estrutura de pastas organizada
- [ ] Sem credenciais ou secrets no repositório
- [ ] Commits com mensagens descritivas
- [ ] Repositório público para portfólio
