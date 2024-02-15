from sqlalchemy import func, desc, and_

from conf.models import Grade, Teacher, Student, Subject, Group
from conf.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    """
    result = (session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade'))
              .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all())
    return result


def select_2():
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    """
    result = (session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade'))
              .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id)
              .order_by(desc('average_grade')).limit(1).all())
    return result


def select_3():
    """
    Знайти середній бал у групах з певного предмета.
    """
    result = (session.query(Group.name, Subject.name, func.round(func.avg(Grade.grade), 2).label('average_grade'))
              .select_from(Grade).join(Student).join(Group).join(Subject).filter(Grade.subject_id == 2)
              .group_by(Group.id, Group.name, Subject.name).all())
    return result


def select_4():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    """
    result = (session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')).select_from(Grade).all())
    return result


def select_5():
    """
    Знайти які курси читає певний викладач.
    """
    result = (session.query(Teacher.fullname, Subject.name).select_from(Subject).join(Teacher).filter(Teacher.id == 2)
              .order_by(Subject.name)).all()
    return result


def select_6():
    """
    Знайти список студентів у певній групі.
    """
    result = (session.query(Group.name, Student.fullname).select_from(Student).join(Group).filter(Group.id == 3)
              .order_by(Student.fullname).all())
    return result


def select_7():
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    """
    result = (session.query(Student.fullname, Group.name, Subject.name, Grade.grade)
              .select_from(Grade).join(Student).join(Group).filter(Group.id == 1).join(Subject).filter(Subject.id == 2)
              .order_by(Student.fullname).all())
    return result


def select_8():
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    """
    result = (session.query(Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade), 2).label('average_grade'))
              .select_from(Grade).join(Subject).join(Teacher).filter(Teacher.id == 2)
              .group_by(Teacher.fullname, Subject.name).order_by('average_grade')
              ).all()
    return result


def select_9():
    """
    Знайти список курсів, які відвідує студент.
    """
    result = (session.query(Student.fullname, Subject.name)
              .select_from(Student).filter(Student.id == 45).join(Grade).join(Subject)
              .group_by(Student.fullname, Subject.name).all())
    return result


def select_10():
    """
    Список курсів, які певному студенту читає певний викладач.
    """
    result = (session.query(Student.fullname, Teacher.fullname, Subject.name)
              .select_from(Student).filter(Student.id == 30).join(Grade).join(Subject).join(Teacher).filter(
        Teacher.id == 2)
              .group_by(Student.fullname, Teacher.fullname, Subject.name).order_by(Teacher.fullname).all()
              )
    return result


def additional_task_1():
    """
    Середній бал, який певний викладач ставить певному студентові.
    """
    result = (
        session.query(Student.fullname, Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade'))
        .select_from(Grade).join(Student).filter(Student.id == 10).join(Subject).join(Teacher).filter(Teacher.id == 2)
        .group_by(Student.fullname, Teacher.fullname).all()
    )
    return result


def additional_task_2():
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    """

    subquery = (session.query(Grade.student_id, Grade.subject_id, func.max(Grade.grade_date).label('last_lesson_date'))
                .group_by(Grade.student_id, Grade.subject_id)
                .subquery())

    result = (session.query(Student.id, Student.fullname.label('student_name'), Group.name.label('group_name'),
                            Subject.name.label('subject_name'), Grade.grade, Grade.grade_date)
              .join(Grade, Student.id == Grade.student_id)
              .join(Subject, Grade.subject_id == Subject.id)
              .join(Group, Student.group_id == Group.id)
              .join(subquery,
                    and_(Grade.student_id == subquery.c.student_id,
                         Grade.subject_id == subquery.c.subject_id,
                         Grade.grade_date == subquery.c.last_lesson_date))
              .filter(Group.id == 1)
              .filter(Subject.id == 3)).all()

    return result


if __name__ == '__main__':
    # print(f'1. 5 студентів із найбільшим середнім балом з усіх предметів:\n', select_1())
    # print(f'2. Студент із найвищим середнім балом з певного предмета:\n', select_2())
    # print(f'3. Середній бал у групах з певного предмета:\n', select_3())
    # print(f'4. Середній бал на потоці (по всій таблиці оцінок):\n', select_4())
    # print(f'5. Які курси читає певний викладач:\n', select_5())
    # print(f'6. Список студентів у певній групі:\n', select_6())
    # print(f'7. Оцінки студентів у окремій групі з певного предмета:\n', select_7())
    # print(f'8. Середній бал, який ставить певний викладач зі своїх предметів:\n', select_8())
    # print(f'9. Список курсів, які відвідує студент:\n', select_9())
    # print(f'10. Список курсів, які певному студенту читає певний викладач:\n', select_10())
    # print(f'Additional Task 1. Середній бал, який певний викладач ставить певному студентові:\n', additional_task_1())
    print(f'Additional Task 2. Оцінки студентів у певній групі з певного предмета на останньому занятті:\n', additional_task_2())
