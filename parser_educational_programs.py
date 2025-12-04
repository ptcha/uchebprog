#!/usr/bin/env python3
"""
Парсер образовательных программ для проекта «Ресоциализация СВО»

Цель: Собрать минимум 50 уникальных образовательных программ государственных 
ВУЗов и ССУЗов РФ с бюджетными местами на 2024/2025 учебный год
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import logging
import random
from urllib.parse import urljoin, urlparse
import re
from typing import List, Dict, Optional, Tuple
import os
from tqdm import tqdm


class EducationalProgramParser:
    def __init__(self, target_list_path: str = "target_list.json"):
        """
        Инициализация парсера образовательных программ
        
        Args:
            target_list_path: путь к JSON-файлу с целевыми кодами специальностей
        """
        self.target_list_path = target_list_path
        self.load_target_list()
        
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('parser.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Настройка сессии с заголовками
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Структура для хранения данных
        self.data = []
        self.stats = {
            'vuzopedia': 0,
            'postupi_online': 0,
            'uchebaru': 0
        }
        
        # Проверка государственности
        self.gov_keywords = [
            'государственный', 'федеральный', 'национальный', 
            'российский', 'рф', 'р.ф.', 'министерство', 'мвд', 'мчс'
        ]
        
        # Проверка бюджетных мест
        self.budget_keywords = ['бюджет', 'кцп', 'целевое', 'грант']
        
        # Основные источники
        self.sources = {
            'vuzopedia': {
                'base_url': 'https://vuzopedia.ru',
                'vuzi_url': 'https://vuzopedia.ru/vuzi',
                'colleges_url': 'https://vuzopedia.ru/colledges'
            },
            'postupi_online': {
                'base_url': 'https://postupi.online',
                'vuzi_url': 'https://postupi.online/vuzi/',
                'spo_url': 'https://postupi.online/specialnosti/'
            }
        }

    def load_target_list(self):
        """Загрузка целевого списка специальностей из JSON-файла"""
        try:
            with open(self.target_list_path, 'r', encoding='utf-8') as f:
                self.target_list = json.load(f)
            self.logger.info(f"Целевой список загружен из {self.target_list_path}")
        except FileNotFoundError:
            self.logger.error(f"Файл {self.target_list_path} не найден")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Ошибка при разборе JSON в файле {self.target_list_path}")
            raise

    def is_government_institution(self, name: str) -> bool:
        """
        Проверка, является ли учреждение государственным
        
        Args:
            name: название учреждения
            
        Returns:
            bool: True если государственное
        """
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in self.gov_keywords)

    def has_budget_places(self, description: str) -> bool:
        """
        Проверка наличия бюджетных мест
        
        Args:
            description: описание с информацией о приеме
            
        Returns:
            bool: True если есть бюджетные места
        """
        desc_lower = description.lower()
        return any(keyword in desc_lower for keyword in self.budget_keywords)

    def extract_region_from_url(self, url: str) -> str:
        """
        Извлечение региона из URL
        
        Args:
            url: URL страницы учреждения
            
        Returns:
            str: название региона
        """
        # Примеры: https://vuzopedia.ru/vuz/123-moscow-state-university
        # или https://postupi.online/vuz/moscow/
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        
        # Ищем регион в URL
        for part in path_parts:
            if 'moscow' in part.lower() or 'москва' in part.lower():
                return 'Москва'
            elif 'spb' in part.lower() or 'санкт-петербург' in part.lower() or 'питер' in part.lower():
                return 'Санкт-Петербург'
            elif 'novosibirsk' in part.lower() or 'новосибирск' in part.lower():
                return 'Новосибирск'
            # Можно добавить больше регионов по необходимости
        
        # Если не нашли в URL, возвращаем общий регион из домена или путь
        return 'Россия'

    def parse_vuzopedia_vuzi(self, fgos_code: str, macrogroup_id: str, macrogroup_name: str) -> List[Dict]:
        """
        Парсинг ВУЗов с Vuzopedia.ru по коду специальности
        
        Args:
            fgos_code: код специальности
            macrogroup_id: ID макрогруппы
            macrogroup_name: название макрогруппы
            
        Returns:
            List[Dict]: список найденных программ
        """
        programs = []
        base_url = self.sources['vuzopedia']['vuzi_url']
        
        try:
            # Формируем URL для поиска по специальности
            search_url = f"{base_url}?speciality={fgos_code.replace('.', '')}"
            
            response = self.session.get(search_url)
            if response.status_code != 200:
                self.logger.warning(f"Не удалось получить данные по {search_url}, статус: {response.status_code}")
                return programs
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем элементы с программами (нужно адаптировать под реальную структуру сайта)
            # Это пример - структура может отличаться
            program_elements = soup.find_all(['div', 'article'], class_=re.compile(r'program|speciality|vuz|item'))
            
            for element in program_elements:
                try:
                    # Извлечение данных из элемента
                    program_name_elem = element.find(['h2', 'h3', 'a'], class_=re.compile(r'title|name|program'))
                    institution_elem = element.find(['h4', 'span'], class_=re.compile(r'institution|university|college'))
                    budget_elem = element.find(['span', 'div'], class_=re.compile(r'budget|places|kcp'))
                    url_elem = element.find('a', href=True)
                    
                    if not all([program_name_elem, institution_elem]):
                        continue
                    
                    program_name = program_name_elem.get_text(strip=True)
                    institution_name = institution_elem.get_text(strip=True)
                    
                    # Проверяем, государственное ли учреждение
                    if not self.is_government_institution(institution_name):
                        continue
                    
                    # Извлекаем бюджетные места
                    budget_seats = 0
                    if budget_elem:
                        budget_text = budget_elem.get_text(strip=True)
                        # Ищем числа в тексте
                        numbers = re.findall(r'\d+', budget_text)
                        if numbers:
                            budget_seats = int(numbers[0])
                    
                    # Извлекаем URL
                    url = ''
                    if url_elem:
                        url = urljoin(self.sources['vuzopedia']['base_url'], url_elem['href'])
                    
                    # Извлекаем регион
                    region = self.extract_region_from_url(url) if url else 'Россия'
                    
                    if budget_seats > 0:  # Только с бюджетными местами
                        program_data = {
                            'id': len(self.data) + len(programs) + 1,
                            'macrogroup_id': macrogroup_id,
                            'macrogroup_name': macrogroup_name,
                            'education_level': 'Высшее' if fgos_code.startswith(('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45')) else 'СПО',
                            'fgos_code': fgos_code,
                            'program_name': program_name,
                            'institution_name': institution_name,
                            'region': region,
                            'budget_seats': budget_seats,
                            'url': url
                        }
                        programs.append(program_data)
                        self.stats['vuzopedia'] += 1
                        
                except Exception as e:
                    self.logger.warning(f"Ошибка при обработке элемента: {e}")
                    continue
            
            # Добавляем задержку между запросами
            time.sleep(random.uniform(2, 3))
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге ВУЗов для {fgos_code}: {e}")
        
        return programs

    def parse_vuzopedia_colleges(self, fgos_code: str, macrogroup_id: str, macrogroup_name: str) -> List[Dict]:
        """
        Парсинг колледжей с Vuzopedia.ru по коду специальности
        
        Args:
            fgos_code: код специальности
            macrogroup_id: ID макрогруппы
            macrogroup_name: название макрогруппы
            
        Returns:
            List[Dict]: список найденных программ
        """
        programs = []
        base_url = self.sources['vuzopedia']['colleges_url']
        
        try:
            # Формируем URL для поиска по специальности
            search_url = f"{base_url}?speciality={fgos_code.replace('.', '')}"
            
            response = self.session.get(search_url)
            if response.status_code != 200:
                self.logger.warning(f"Не удалось получить данные по {search_url}, статус: {response.status_code}")
                return programs
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем элементы с программами
            program_elements = soup.find_all(['div', 'article'], class_=re.compile(r'program|speciality|college|item'))
            
            for element in program_elements:
                try:
                    # Извлечение данных из элемента
                    program_name_elem = element.find(['h2', 'h3', 'a'], class_=re.compile(r'title|name|program'))
                    institution_elem = element.find(['h4', 'span'], class_=re.compile(r'institution|university|college'))
                    budget_elem = element.find(['span', 'div'], class_=re.compile(r'budget|places|kcp'))
                    url_elem = element.find('a', href=True)
                    
                    if not all([program_name_elem, institution_elem]):
                        continue
                    
                    program_name = program_name_elem.get_text(strip=True)
                    institution_name = institution_elem.get_text(strip=True)
                    
                    # Проверяем, государственное ли учреждение
                    if not self.is_government_institution(institution_name):
                        continue
                    
                    # Извлекаем бюджетные места
                    budget_seats = 0
                    if budget_elem:
                        budget_text = budget_elem.get_text(strip=True)
                        # Ищем числа в тексте
                        numbers = re.findall(r'\d+', budget_text)
                        if numbers:
                            budget_seats = int(numbers[0])
                    
                    # Извлекаем URL
                    url = ''
                    if url_elem:
                        url = urljoin(self.sources['vuzopedia']['base_url'], url_elem['href'])
                    
                    # Извлекаем регион
                    region = self.extract_region_from_url(url) if url else 'Россия'
                    
                    if budget_seats > 0:  # Только с бюджетными местами
                        program_data = {
                            'id': len(self.data) + len(programs) + 1,
                            'macrogroup_id': macrogroup_id,
                            'macrogroup_name': macrogroup_name,
                            'education_level': 'СПО',
                            'fgos_code': fgos_code,
                            'program_name': program_name,
                            'institution_name': institution_name,
                            'region': region,
                            'budget_seats': budget_seats,
                            'url': url
                        }
                        programs.append(program_data)
                        self.stats['vuzopedia'] += 1
                        
                except Exception as e:
                    self.logger.warning(f"Ошибка при обработке элемента: {e}")
                    continue
            
            # Добавляем задержку между запросами
            time.sleep(random.uniform(2, 3))
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге колледжей для {fgos_code}: {e}")
        
        return programs

    def parse_postupi_online(self, fgos_code: str, macrogroup_id: str, macrogroup_name: str, education_level: str) -> List[Dict]:
        """
        Парсинг с Postupi.online
        
        Args:
            fgos_code: код специальности
            macrogroup_id: ID макрогруппы
            macrogroup_name: название макрогруппы
            education_level: уровень образования ('Высшее' или 'СПО')
            
        Returns:
            List[Dict]: список найденных программ
        """
        programs = []
        
        try:
            if education_level == 'Высшее':
                base_url = self.sources['postupi_online']['vuzi_url']
                search_url = f"{base_url}?speciality={fgos_code}"
            else:  # СПО
                base_url = self.sources['postupi_online']['spo_url']
                search_url = f"{base_url}?code={fgos_code}"
            
            response = self.session.get(search_url)
            if response.status_code != 200:
                self.logger.warning(f"Не удалось получить данные по {search_url}, статус: {response.status_code}")
                return programs
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем элементы с программами (адаптировать под реальную структуру)
            program_elements = soup.find_all(['div', 'article'], class_=re.compile(r'program|speciality|university|college|item'))
            
            for element in program_elements:
                try:
                    # Извлечение данных из элемента
                    program_name_elem = element.find(['h2', 'h3', 'a'], class_=re.compile(r'title|name|program'))
                    institution_elem = element.find(['h4', 'span'], class_=re.compile(r'institution|university|college'))
                    budget_elem = element.find(['span', 'div'], class_=re.compile(r'budget|places|kcp'))
                    url_elem = element.find('a', href=True)
                    
                    if not all([program_name_elem, institution_elem]):
                        continue
                    
                    program_name = program_name_elem.get_text(strip=True)
                    institution_name = institution_elem.get_text(strip=True)
                    
                    # Проверяем, государственное ли учреждение
                    if not self.is_government_institution(institution_name):
                        continue
                    
                    # Извлекаем бюджетные места
                    budget_seats = 0
                    if budget_elem:
                        budget_text = budget_elem.get_text(strip=True)
                        # Ищем числа в тексте
                        numbers = re.findall(r'\d+', budget_text)
                        if numbers:
                            budget_seats = int(numbers[0])
                    
                    # Извлекаем URL
                    url = ''
                    if url_elem:
                        url = urljoin(self.sources['postupi_online']['base_url'], url_elem['href'])
                    
                    # Извлекаем регион
                    region = self.extract_region_from_url(url) if url else 'Россия'
                    
                    if budget_seats > 0:  # Только с бюджетными местами
                        program_data = {
                            'id': len(self.data) + len(programs) + 1,
                            'macrogroup_id': macrogroup_id,
                            'macrogroup_name': macrogroup_name,
                            'education_level': education_level,
                            'fgos_code': fgos_code,
                            'program_name': program_name,
                            'institution_name': institution_name,
                            'region': region,
                            'budget_seats': budget_seats,
                            'url': url
                        }
                        programs.append(program_data)
                        self.stats['postupi_online'] += 1
                        
                except Exception as e:
                    self.logger.warning(f"Ошибка при обработке элемента: {e}")
                    continue
            
            # Добавляем задержку между запросами
            time.sleep(random.uniform(2, 3))
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге с Postupi.online для {fgos_code}: {e}")
        
        return programs

    def parse_all_sources(self, min_programs: int = 50) -> pd.DataFrame:
        """
        Основной метод для парсинга всех источников
        
        Args:
            min_programs: минимальное количество программ для сбора
            
        Returns:
            pd.DataFrame: собранные данные
        """
        self.logger.info("Начало сбора данных...")
        
        # Сначала пытаемся собрать данные с Vuzopedia
        for macrogroup_id, macrogroup_data in self.target_list.items():
            macrogroup_name = macrogroup_data['name']
            
            # Парсинг ВО кодов
            for code in tqdm(macrogroup_data['vo_codes'], desc=f"Парсинг ВО для макрогруппы {macrogroup_id}"):
                programs = self.parse_vuzopedia_vuzi(code, macrogroup_id, macrogroup_name)
                self.data.extend(programs)
                
                if len(self.data) >= min_programs:
                    break
            
            if len(self.data) >= min_programs:
                break
                
            # Парсинг СПО кодов
            for code in tqdm(macrogroup_data['spo_codes'], desc=f"Парсинг СПО для макрогруппы {macrogroup_id}"):
                programs = self.parse_vuzopedia_colleges(code, macrogroup_id, macrogroup_name)
                self.data.extend(programs)
                
                if len(self.data) >= min_programs:
                    break
            
            if len(self.data) >= min_programs:
                break
        
        # Если недостаточно данных, используем Postupi.online
        if len(self.data) < min_programs:
            self.logger.info(f"Собрано {len(self.data)} программ, требуется {min_programs}. Переход к Postupi.online...")
            
            for macrogroup_id, macrogroup_data in self.target_list.items():
                macrogroup_name = macrogroup_data['name']
                
                # Парсинг ВО кодов
                for code in tqdm(macrogroup_data['vo_codes'], desc=f"Postupi ВО для макрогруппы {macrogroup_id}"):
                    programs = self.parse_postupi_online(code, macrogroup_id, macrogroup_name, 'Высшее')
                    self.data.extend(programs)
                    
                    if len(self.data) >= min_programs:
                        break
                
                if len(self.data) >= min_programs:
                    break
                    
                # Парсинг СПО кодов
                for code in tqdm(macrogroup_data['spo_codes'], desc=f"Postupi СПО для макрогруппы {macrogroup_id}"):
                    programs = self.parse_postupi_online(code, macrogroup_id, macrogroup_name, 'СПО')
                    self.data.extend(programs)
                    
                    if len(self.data) >= min_programs:
                        break
                
                if len(self.data) >= min_programs:
                    break
        
        # Создаем DataFrame
        df = pd.DataFrame(self.data)
        
        # Удаление дубликатов
        if not df.empty:
            df.drop_duplicates(subset=['fgos_code', 'institution_name', 'program_name'], keep='first', inplace=True)
            
            # Обновляем ID после удаления дубликатов
            df['id'] = range(1, len(df) + 1)
        
        self.logger.info(f"Сбор завершен. Всего собрано: {len(df)} программ")
        self.logger.info(f"Статистика по источникам: {self.stats}")
        
        return df

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обработка и очистка данных
        
        Args:
            df: исходный DataFrame
            
        Returns:
            pd.DataFrame: обработанный DataFrame
        """
        if df.empty:
            return df
        
        # Приведение бюджетных мест к единому формату
        df['budget_seats'] = df['budget_seats'].apply(
            lambda x: str(x) if pd.notna(x) and x != 0 else 'Нет'
        )
        
        # Приведение региона к стандартному формату
        def standardize_region(region):
            if pd.isna(region):
                return 'Россия'
            # Убираем лишние слова
            region = region.replace('Россия, ', '').replace('Российская Федерация, ', '')
            return region.strip()
        
        df['region'] = df['region'].apply(standardize_region)
        
        # Убедимся, что все 10 макрогрупп представлены
        macrogroups_present = set(df['macrogroup_id'].unique())
        all_macrogroups = set(self.target_list.keys())
        
        missing_groups = all_macrogroups - macrogroups_present
        if missing_groups:
            self.logger.warning(f"Не найдены программы для макрогрупп: {missing_groups}")
        
        return df

    def save_results(self, df: pd.DataFrame, filename: str = 'educational_programs_2024.csv'):
        """
        Сохранение результатов в CSV
        
        Args:
            df: DataFrame с данными
            filename: имя файла для сохранения
        """
        try:
            df.to_csv(filename, index=False, encoding='utf-8')
            self.logger.info(f"Результаты сохранены в {filename}")
            
            # Создание отчета
            report = self.generate_report(df)
            report_filename = filename.replace('.csv', '_report.txt')
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(report)
            self.logger.info(f"Отчет сохранен в {report_filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении файла: {e}")

    def generate_report(self, df: pd.DataFrame) -> str:
        """
        Генерация отчета о сборе данных
        
        Args:
            df: DataFrame с данными
            
        Returns:
            str: текст отчета
        """
        report = []
        report.append("=" * 50)
        report.append("ОТЧЕТ О СБОРЕ ОБРАЗОВАТЕЛЬНЫХ ПРОГРАММ")
        report.append("=" * 50)
        report.append(f"Общее количество собранных программ: {len(df)}")
        report.append("")
        
        report.append("Распределение по источникам:")
        for source, count in self.stats.items():
            report.append(f"  {source}: {count}")
        report.append("")
        
        report.append("Распределение по макрогруппам:")
        if not df.empty:
            macrogroup_counts = df['macrogroup_id'].value_counts().sort_index()
            for mg_id, count in macrogroup_counts.items():
                name = self.target_list.get(mg_id, {}).get('name', 'Неизвестно')
                report.append(f"  {mg_id} - {name}: {count}")
        report.append("")
        
        report.append("Распределение по уровням образования:")
        if not df.empty:
            edu_level_counts = df['education_level'].value_counts()
            for level, count in edu_level_counts.items():
                report.append(f"  {level}: {count}")
        report.append("")
        
        report.append("Распределение по регионам (топ-10):")
        if not df.empty:
            region_counts = df['region'].value_counts().head(10)
            for region, count in region_counts.items():
                report.append(f"  {region}: {count}")
        report.append("")
        
        report.append("Статистика по бюджетным местам:")
        if not df.empty:
            budget_stats = df['budget_seats'].value_counts().head(10)
            for seats, count in budget_stats.items():
                report.append(f"  {seats} мест: {count} программ")
        
        return "\n".join(report)

    def run(self, min_programs: int = 50, output_file: str = 'educational_programs_2024.csv'):
        """
        Запуск полного процесса парсинга
        
        Args:
            min_programs: минимальное количество программ для сбора
            output_file: имя файла для сохранения
        """
        start_time = time.time()
        
        try:
            # Сбор данных из всех источников
            df = self.parse_all_sources(min_programs)
            
            # Обработка данных
            df = self.process_data(df)
            
            # Сохранение результатов
            self.save_results(df, output_file)
            
            # Время выполнения
            execution_time = time.time() - start_time
            self.logger.info(f"Время выполнения: {execution_time:.2f} секунд")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Ошибка при выполнении парсинга: {e}")
            raise


def main():
    """Основная функция для запуска парсера"""
    parser = EducationalProgramParser()
    
    # Запуск парсинга
    df = parser.run(min_programs=50, output_file='educational_programs_2024.csv')
    
    # Вывод первых 5 строк для демонстрации
    print("\nПервые 5 строк результата:")
    print(df.head().to_string())
    
    # Вывод статистики
    print(f"\nОбщее количество программ: {len(df)}")
    print(f"Уникальных макрогрупп: {df['macrogroup_id'].nunique()}")
    print(f"Уникальных регионов: {df['region'].nunique()}")


if __name__ == "__main__":
    main()