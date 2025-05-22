import os
import re
import json
import google.generativeai as genai
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class ScriptData:
    name: str
    path: str
    code: str
    connections: Dict
    inputs: List[Dict] = None
    outputs: List[Dict] = None
    combinable_with: List[str] = None

class ScriptAnalyzer:
    def __init__(self, base_dir: str, api_key: str):
        self.base_dir = base_dir
        self.scripts: List[ScriptData] = []
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def find_scripts(self) -> List[str]:
        return [os.path.join(r, f) for r, _, files in os.walk(self.base_dir) 
                for f in files if f.endswith('.py')]
    
    def extract_connections(self, code: str) -> Dict:
        patterns = {
            'postgres': r"psycopg2\.connect.*?database\s*=\s*['\"]([^'\"]+)",
            'mysql': r"mysql\.connector\.connect.*?database\s*=\s*['\"]([^'\"]+)",
            'mongodb': r"get_collection\(['\"]([^'\"]+)['\"]\)"
        }
        return {db: re.findall(pattern, code) for db, pattern in patterns.items()}
    
    def analyze_with_llm(self, script: ScriptData) -> Dict:
        prompt = f"""
        Extraia inputs/outputs deste script Python:
        
        ```python
        {script.code[:3000]}
        ```
        
        Retorne JSON:
        {{
            "inputs": [{{"source": "tabela", "columns": ["col1"]}}, ...],
            "outputs": [{{"destination": "tabela", "operation": "INSERT/UPDATE"}}, ...],
            "combinable_with": ["tabela1", "tabela2"]
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            json_match = re.search(r"```json\n(.*?)\n```", response.text, re.DOTALL)
            return json.loads(json_match.group(1) if json_match else response.text)
        except:
            return {"inputs": [], "outputs": [], "combinable_with": []}
    
    def analyze_script(self, path: str) -> ScriptData:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            code = f.read()
        
        script = ScriptData(
            name=os.path.basename(path),
            path=path,
            code=code,
            connections=self.extract_connections(code)
        )
        
        llm_result = self.analyze_with_llm(script)
        script.inputs = llm_result.get('inputs', [])
        script.outputs = llm_result.get('outputs', [])
        script.combinable_with = llm_result.get('combinable_with', [])
        
        return script
    
    def find_combinations(self) -> List[Dict]:
        # Agrupa scripts por tabelas que manipulam
        table_groups = {}
        for script in self.scripts:
            for output in script.outputs:
                table = output.get('destination', '')
                if table:
                    if table not in table_groups:
                        table_groups[table] = []
                    table_groups[table].append(script.name)
        
        # Retorna grupos com mais de 1 script
        return [{"table": table, "scripts": scripts} 
                for table, scripts in table_groups.items() if len(scripts) > 1]
    
    def generate_unified_sql(self, combination: Dict) -> str:
        scripts_data = [s for s in self.scripts if s.name in combination['scripts']]
        
        prompt = f"""
        Gere SQL unificado para combinar estes scripts que alteram a tabela {combination['table']}:
        
        {json.dumps([{"name": s.name, "inputs": s.inputs, "outputs": s.outputs} for s in scripts_data], indent=2)}
        
        Retorne apenas o SQL:
        ```sql
        -- SQL aqui
        ```
        """
        
        try:
            response = self.model.generate_content(prompt)
            sql_match = re.search(r"```sql\n(.*?)\n```", response.text, re.DOTALL)
            return sql_match.group(1) if sql_match else response.text
        except:
            return f"-- Erro ao gerar SQL para {combination['table']}"
    
    def run_analysis(self) -> Dict:
        print("Analisando scripts...")
        
        for path in self.find_scripts():
            script = self.analyze_script(path)
            self.scripts.append(script)
            print(f"✓ {script.name}")
        
        combinations = self.find_combinations()
        
        results = {
            "scripts": len(self.scripts),
            "combinations": combinations,
            "unified_sql": {}
        }
        
        print(f"\nCombinações encontradas: {len(combinations)}")
        for combo in combinations:
            sql = self.generate_unified_sql(combo)
            results["unified_sql"][combo["table"]] = sql
            print(f"✓ SQL gerado para {combo['table']}")
        
        return results
    
    def save_results(self, filename: str = "analysis_results.json"):
        results = self.run_analysis()
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResultados salvos em {filename}")

# Uso
if __name__ == "__main__":
    analyzer = ScriptAnalyzer(
        base_dir="/home/pedro/logic-test-main/sources/rpa",
        api_key="AIzaSyAx6iH1WuCJsb50_LhOahBr2OQnk3TqIq4"
    )
    analyzer.save_results()