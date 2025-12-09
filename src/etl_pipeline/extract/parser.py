import re
from bs4 import BeautifulSoup
from datetime import datetime
from . import config

def parse_thread_list(html):
    """
    Parses the user submissions page to find 'Who is hiring?' threads.
    Returns (threads_list, has_more_bool)
    """
    soup = BeautifulSoup(html, 'html.parser')
    threads = []
    
    # Pattern: "Ask HN: Who is hiring? (<Month> <Year>)"
    pattern = re.compile(r"Ask HN: Who is hiring\? \((?P<month>\w+) (?P<year>\d{4})\)")
    
    for item in soup.select('tr.athing'):
        link = item.select_one('span.titleline > a')
        if not link:
            # Fallback for older HN layout
            link = item.select_one('a.titlelink')
        
        if not link:
            continue
            
        title = link.get_text()
        match = pattern.search(title)
        if match:
            thread_id = item['id']
            month_str = f"{match.group('month')} {match.group('year')}"
            try:
                date_obj = datetime.strptime(month_str, "%B %Y")
                thread_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
                
            threads.append({
                'id': thread_id,
                'title': title,
                'thread_date': thread_date,
                'url': f"{config.BASE_URL}/item?id={thread_id}"
            })
            
    more_link = soup.select_one('a.morelink')
    next_page_params = None
    if more_link:
        next_page_params = more_link.get('href')
    
    return threads, next_page_params

def parse_comments(html, thread_date):
    """
    Parses a thread page to extract top-level comments.
    Returns (comments_list, has_more_bool)
    """
    soup = BeautifulSoup(html, 'html.parser')
    comments = []
    
    # Find all comment rows
    comment_rows = soup.select('tr.athing.comtr')
    
    for row in comment_rows:
        # Check indentation to ensure top-level
        ind_cell = row.select_one('td.ind')
        if ind_cell:
            img = ind_cell.select_one('img')
            # Top level comments have width=0
            if img and img.get('width') != '0':
                continue 
        
        # Extract data
        comment_id = row['id']
        
        user_elem = row.select_one('a.hnuser')
        user = user_elem.get_text() if user_elem else "unknown"
        
        text_elem = row.select_one('div.commtext')
        if text_elem:
            # Preserve newlines
            raw_text = text_elem.get_text(separator='\n').strip()
            
            # Remove "reply" text if it gets captured (usually it's in a separate div or link, but good to be safe)
            # Actually get_text might capture the "reply" link text if it's inside div.commtext?
            # Usually "reply" is in a separate div.reply, not inside commtext.
        else:
            raw_text = ""
            
        # Timestamp/Permalink
        age_elem = row.select_one('span.age a')
        url = ""
        if age_elem:
            href = age_elem.get('href')
            url = f"{config.BASE_URL}/{href}"
            
        comments.append({
            'id': comment_id,
            'thread_date': thread_date,
            'raw_text': raw_text,
            'user': user,
            'url': url
        })
        
    more_link = soup.select_one('a.morelink')
    has_more = more_link is not None
    
    return comments, has_more
