#!/usr/bin/env python3
"""
Скрипт для автоматического извлечения данных из публичной Google-таблицы
и генерации HTML-карточек для вставки на статический сайт.

Как использовать:
1. Запустите скрипт: python generate_programs.py
2. Скрипт создаст файл programs.html с карточками программ
3. Вставьте содержимое файла programs.html в блок с id="programs-container" на вашем сайте

Структура исходной таблицы:
- program_name: Название программы
- education_level: Уровень образования
- institution_name: Название учебного заведения
- region: Регион
- budget_seats: Информация о бюджетных местах
- url: Ссылка на программу
"""

import csv
import html
from datetime import datetime
import sys
import json
import re


def generate_program_card(program_data):
    """
    Генерирует JavaScript объект для одной образовательной программы
    в формате, подходящем для использования в index.html.
    
    Args:
        program_data (dict): Данные программы с ключами:
            - 'program_name' (Название программы)
            - 'education_level' (Уровень образования)
            - 'institution_name' (Название учреждения)
            - 'region' (Регион)
            - 'budget_seats' (Бюджетные места)
            - 'url' (Ссылка на программу)
            - 'fgos_code' (Код специальности)
            - 'macrogroup_name' (Название макрогруппы)
    
    Returns:
        str: JavaScript объект программы
    """
    # Экранируем HTML-специфичные символы
    title = html.escape(program_data.get('program_name', '').strip())
    education_level = html.escape(program_data.get('education_level', '').strip())
    institution = html.escape(program_data.get('institution_name', '').strip())
    region = html.escape(program_data.get('region', '').strip())
    budget_seats = html.escape(program_data.get('budget_seats', '').strip())
    url = html.escape(program_data.get('url', '').strip())
    fgos_code = html.escape(program_data.get('fgos_code', '').strip())
    macrogroup_name = html.escape(program_data.get('macrogroup_name', '').strip())
    
    # Определяем уровень образования
    level = 'VO' if 'СПО' not in education_level else 'SPO'
    level_readable = 'Бакалавриат' if 'СПО' not in education_level else 'Колледж'
    
    # Обрабатываем бюджетные места для числового значения
    places = 0
    if budget_seats:
        numbers = re.findall(r'\d+', budget_seats)
        if numbers:
            places = int(numbers[0])
        elif 'Есть бюджетные места' in budget_seats:
            places = 1  # условное значение
    
    # Формируем теги
    tags = ['Бюджет']  # по умолчанию добавляем бюджет
    
    # Формируем описание
    description = f"{macrogroup_name}" if macrogroup_name else "Описание программы загружается..."
    
    # Создаем JavaScript объект программы
    program_obj = {
        'title': title if title != '' else 'Без названия',
        'fgos_code': fgos_code if fgos_code != '' else '—',
        'fgos_name': macrogroup_name,
        'institution': institution if institution != '' else 'Не указан',
        'district': region if region != '' else 'РФ',
        'level': level,
        'level_readable': level_readable,
        'base': '11 классов',  # по умолчанию
        'duration': '4 года',  # по умолчанию
        'category': macrogroup_name if macrogroup_name else 'Общее',
        'tags': tags,
        'places': places,
        'salary': 'по итогам',
        'desc': description,
        'color': 'blue',  # по умолчанию
        'quiz_cat': 'Tech',  # по умолчанию
        'url': url  # добавляем URL
    }
    
    # Возвращаем JSON-подобное представление объекта
    return json.dumps(program_obj, ensure_ascii=False)


def fetch_csv_data(csv_url):
    """
    Загружает CSV-данные из указанного URL.
    
    Args:
        csv_url (str): URL для экспорта CSV из Google Таблицы
    
    Returns:
        list: Список словарей, представляющих строки таблицы
    """
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        # Декодируем содержимое CSV
        csv_content = response.content.decode('utf-8')
        
        # Парсим CSV
        lines = csv_content.splitlines()
        reader = csv.DictReader(lines)
        
        return list(reader)
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке CSV: {e}")
        return None
    except UnicodeDecodeError:
        # Пробуем декодировать с другой кодировкой
        try:
            csv_content = response.content.decode('utf-8-sig')
            lines = csv_content.splitlines()
            reader = csv.DictReader(lines)
            return list(reader)
        except Exception as e:
            print(f"Ошибка при декодировании CSV: {e}")
            return None
    except Exception as e:
        print(f"Неизвестная ошибка при обработке CSV: {e}")
        return None




def main():
    # Путь к локальному CSV файлу
    csv_file_path = "table.csv"
    
    print("Загрузка данных из локального CSV файла...")
    # Читаем локальный CSV файл
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        # Парсим CSV
        lines = csv_content.splitlines()
        reader = csv.DictReader(lines)
        data = list(reader)
        
    except FileNotFoundError:
        print(f"Файл {csv_file_path} не найден.")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении CSV файла: {e}")
        sys.exit(1)
    
    if not data:
        print("Таблица пуста или не содержит данных.")
        return
    
    print(f"Загружено {len(data)} записей из таблицы.")
    
    # Генерируем JavaScript массив программ
    js_programs = []
    for row in data:
        # Проверяем, что основные поля присутствуют
        required_fields = ['program_name', 'url']
        missing_fields = [field for field in required_fields if field not in row or not row[field].strip()]
        if missing_fields:
            print(f"Предупреждение: В строке отсутствуют или пусты обязательные поля: {missing_fields}. Пропускаем эту строку.")
            continue
        
        program_obj = generate_program_card(row)
        js_programs.append(program_obj)
    
    if not js_programs:
        print("Не удалось сгенерировать ни одной программы из-за отсутствия необходимых полей в данных.")
        return
    
    # Формируем JavaScript код с массивом программ
    current_date = datetime.now().strftime("%d.%m.%Y")
    js_code = f"""// Сгенерировано автоматически {current_date}
const programsFromCSV = [
{',\n'.join(js_programs)}
];"""
    
    # Сохраняем в файл
    with open("programs.js", "w", encoding="utf-8") as f:
        f.write(js_code)
    
    print(f"JavaScript массив программ успешно сгенерирован и сохранен в файл programs.js")
    print(f"Сгенерировано {len(js_programs)} программ.")


if __name__ == "__main__":
    main()