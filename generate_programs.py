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

import requests
import csv
import html
from datetime import datetime
import sys


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


def generate_program_card(program_data):
    """
    Генерирует HTML-карточку для одной образовательной программы.
    
    Args:
        program_data (dict): Данные программы с ключами:
            - 'program_name' (Название программы)
            - 'education_level' (Уровень образования)
            - 'institution_name' (Название учреждения)
            - 'region' (Регион)
            - 'budget_seats' (Бюджетные места)
            - 'url' (Ссылка на программу)
    
    Returns:
        str: HTML-разметка карточки программы
    """
    # Экранируем HTML-специфичные символы
    title = html.escape(program_data.get('program_name', '').strip())
    education_level = html.escape(program_data.get('education_level', '').strip())
    institution = html.escape(program_data.get('institution_name', '').strip())
    region = html.escape(program_data.get('region', '').strip())
    budget_seats = html.escape(program_data.get('budget_seats', '').strip())
    url = html.escape(program_data.get('url', '').strip())
    
    # Формируем описание на основе доступных данных
    description_parts = []
    if education_level:
        description_parts.append(f"Уровень: {education_level}")
    if institution:
        description_parts.append(f"Учреждение: {institution}")
    if region:
        description_parts.append(f"Регион: {region}")
    if budget_seats:
        description_parts.append(f"Бюджетные места: {budget_seats}")
    
    description = ". ".join(description_parts)
    
    # Формируем HTML-карточку
    card_html = f"""<div class="program-card">
  <h3 class="program-title">{title}</h3>
  <p class="program-description">{description}</p>"""
    
    # Добавляем дополнительную информацию, если доступна
    if education_level:
        card_html += f"\n  <span class=\"program-duration\">Уровень: {education_level}</span>"
    
    # Добавляем цену, если она не пустая (в реальных данных стоимость отсутствует)
    # Но оставляем поле для совместимости с шаблоном
    
    # Добавляем ссылку
    card_html += f"""
  <a href="{url}" class="program-link">Подробнее</a>
</div>"""
    
    return card_html


def main():
    # URL для экспорта CSV из Google Таблицы
    csv_url = "https://docs.google.com/spreadsheets/d/12GNjcomxxU4NbXKwQ1Jq_eG7M7PLUlDlKAnBO4qwu5g/export?format=csv&gid=0"
    
    print("Загрузка данных из Google Таблицы...")
    data = fetch_csv_data(csv_url)
    
    if data is None:
        print("Не удалось загрузить данные из таблицы.")
        sys.exit(1)
    
    if not data:
        print("Таблица пуста или не содержит данных.")
        return
    
    print(f"Загружено {len(data)} записей из таблицы.")
    
    # Генерируем HTML-карточки
    html_cards = []
    for row in data:
        # Проверяем, что основные поля присутствуют
        required_fields = ['program_name', 'url']
        missing_fields = [field for field in required_fields if field not in row or not row[field].strip()]
        if missing_fields:
            print(f"Предупреждение: В строке отсутствуют или пусты обязательные поля: {missing_fields}. Пропускаем эту строку.")
            continue
        
        card_html = generate_program_card(row)
        html_cards.append(card_html)
    
    if not html_cards:
        print("Не удалось сгенерировать ни одной карточки из-за отсутствия необходимых полей в данных.")
        return
    
    # Формируем итоговый HTML-файл
    current_date = datetime.now().strftime("%d.%m.%Y")
    header_comment = f"<!-- Сгенерировано автоматически {current_date} -->"
    full_html = header_comment + "\n" + "\n".join(html_cards)
    
    # Сохраняем в файл
    with open("programs.html", "w", encoding="utf-8") as f:
        f.write(full_html)
    
    print(f"HTML-карточки успешно сгенерированы и сохранены в файл programs.html")
    print(f"Сгенерировано {len(html_cards)} карточек.")


if __name__ == "__main__":
    main()